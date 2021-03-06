/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util.btree;

import com.google.common.base.Preconditions;
import io.pravega.common.util.BitConverter;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.IllegalDataFormatException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * B+Tree Page containing raw data. Wraps around a ByteArraySegment and formats it using a special layout.
 *
 * Format: Header|Data|Footer
 * * Header: FormatVersion(1)|Flags(1)|Id(4)|Count(4)
 * * Data: List{Key(KL)|Value(VL)}
 * * Footer: Id(4)
 *
 * The Header contains:
 * * The current format version
 * * A set of flags that apply to this Page. Currently the only one used is to identify if it is an Index or Leaf page.
 * * A randomly generated Page Identifier.
 * * The number of items in the Page.
 *
 * The Data contains:
 * * A list of Keys and Values, sorted by Key (using ByteArrayComparator). The length of this list is defined in the Header.
 *
 * The Footer contains:
 * * The same Page Identifier as in the Header. When wrapping an existing ByteArraySegment, this value is matched to the
 * one in the Header to ensure the Page was loaded correctly.
 *
 */
@NotThreadSafe
class BTreePage {
    //region Format

    /**
     * Format Version related fields. The version itself is the first byte of the serialization. When we will have to
     * support multiple versions, we will need to read this byte and choose the appropriate deserialization approach.
     * We cannot use VersionedSerializer in here - doing so would prevent us from efficiently querying and modifying the
     * page contents itself, as it would force us to load everything in memory (as objects) and then reserialize them.
     */
    private static final byte CURRENT_VERSION = 0;
    private static final int VERSION_OFFSET = 0;
    private static final int VERSION_LENGTH = 1; // Maximum 256 versions.

    /**
     * Page Flags.
     */
    private static final int FLAGS_OFFSET = VERSION_OFFSET + VERSION_LENGTH;
    private static final int FLAGS_LENGTH = 1; // Maximum 8 flags.
    private static final byte FLAG_NONE = 0;
    private static final byte FLAG_INDEX_PAGE = 1; // If set, indicates this is an Index Page; if not, it's a Leaf Page.

    /**
     * Page Id: Randomly generated Integer that is written both in the Header and Footer. This enables us to validate
     * that whatever ByteArraySegment we receive for deserialization has the appropriate length.
     */
    private static final int ID_OFFSET = FLAGS_OFFSET + FLAGS_LENGTH;
    private static final int ID_LENGTH = 4;

    /**
     * Element Count.
     */
    private static final int COUNT_OFFSET = ID_OFFSET + ID_LENGTH;
    private static final int COUNT_LENGTH = 4; // Allows overflowing, but needed in order to do splits.

    /**
     * Data (contents).
     */
    private static final int DATA_OFFSET = COUNT_OFFSET + COUNT_LENGTH;

    /**
     * Footer: Contains just the Page Id, which should match the value written in the Header.
     */
    private static final int FOOTER_LENGTH = ID_LENGTH;

    //endregion

    //region Members

    private static final ByteArrayComparator KEY_COMPARATOR = new ByteArrayComparator();
    private static final Random ID_GENERATOR = new Random();

    /**
     * The entire ByteArraySegment that makes up this BTreePage. This includes Header, Data and Footer.
     */
    @Getter
    private ByteArraySegment contents;
    /**
     * The Footer section of the BTreePage ByteArraySegment.
     */
    private ByteArraySegment header;
    /**
     * The Data section of the BTreePage ByteArraySegment.
     */
    private ByteArraySegment data;
    /**
     * The Footer section of the BTreePage ByteArraySegment.
     */
    private ByteArraySegment footer;
    @Getter
    private final Config config;
    /**
     * The number of items in this BTreePage as reflected in its header.
     */
    @Getter
    private int count;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of an empty BTreePage class..
     *
     * @param config Page Configuration.
     */
    BTreePage(Config config) {
        this(config, new ByteArraySegment(new byte[DATA_OFFSET + FOOTER_LENGTH]), false);
        formatHeaderAndFooter(this.count, ID_GENERATOR.nextInt());
    }

    /**
     * Creates a new instance of the BTreePage class wrapping an existing ByteArraySegment.
     *
     * @param config   Page Configuration.
     * @param contents The ByteArraySegment to wrap. Changes to this BTreePage may change the values in the array backing
     *                 this ByteArraySegment.
     * @throws IllegalDataFormatException If the given contents is not a valid BTreePage format.
     */
    BTreePage(Config config, ByteArraySegment contents) {
        this(config, contents, true);
    }

    /**
     * Creates a new instance of the BTreePage class wrapping the given Data Items (no header or footer).
     *
     * @param config Page Configuration.
     * @param count  Number of items in data.
     * @param data   A ByteArraySegment containing a list of Key-Value pairs to include. The contents of this ByteArraySegment
     *               will be copied into a new buffer, so changes to this BTreePage will not affect it.
     */
    private BTreePage(Config config, int count, ByteArraySegment data) {
        this(config, new ByteArraySegment(new byte[DATA_OFFSET + data.getLength() + FOOTER_LENGTH]), false);
        Preconditions.checkArgument(count * config.entryLength == data.getLength(), "Unexpected data length given the count.");
        formatHeaderAndFooter(count, ID_GENERATOR.nextInt());
        this.data.copyFrom(data, 0, data.getLength());
    }

    /**
     * Creates a new instance of the BTreePage class wrapping an existing ByteArraySegment.
     *
     * @param config   Page Configuration.
     * @param contents The ByteArraySegment to wrap. Changes to this BTreePage may change the values in the array backing
     *                 this ByteArraySegment.
     * @param validate If true, will perform validation.
     * @throws IllegalDataFormatException If the given contents is not a valid BTreePage format and validate == true.
     */
    private BTreePage(@NonNull Config config, @NonNull ByteArraySegment contents, boolean validate) {
        Preconditions.checkArgument(!contents.isReadOnly(), "Cannot wrap a read-only ByteArraySegment.");
        this.config = config;
        this.contents = contents;
        this.header = contents.subSegment(0, DATA_OFFSET);
        this.data = contents.subSegment(DATA_OFFSET, contents.getLength() - DATA_OFFSET - FOOTER_LENGTH);
        this.footer = contents.subSegment(contents.getLength() - FOOTER_LENGTH, FOOTER_LENGTH);
        if (validate) {
            int headerId = getHeaderId();
            int footerId = getFooterId();
            if (headerId != footerId) {
                throw new IllegalDataFormatException("Invalid Page Format (id mismatch). HeaderId=%s, FooterId=%s.", headerId, footerId);
            }
        }

        // Cache the count value. It's used a lot.
        this.count = BitConverter.readInt(this.header, COUNT_OFFSET);
    }

    /**
     * Formats the Header and Footer of this BTreePage with the given information.
     *
     * @param itemCount The number of items in this BTreePage.
     * @param id        The Id of this BTreePage.
     */
    private void formatHeaderAndFooter(int itemCount, int id) {
        // Header.
        this.header.set(VERSION_OFFSET, CURRENT_VERSION);
        this.header.set(FLAGS_OFFSET, getFlags(this.config.isIndexPage ? FLAG_INDEX_PAGE : FLAG_NONE));
        setHeaderId(id);
        setCount(itemCount);

        // Matching footer.
        setFooterId(id);
    }

    //endregion

    //region Operations

    /**
     * Determines whether the given ByteArraySegment represents an Index Page
     *
     * @param pageContents The ByteArraySegment to check.
     * @return True if Index Page, False if Leaf page.
     * @throws IllegalDataFormatException If the given contents is not a valid BTreePage format.
     */
    static boolean isIndexPage(@NonNull ByteArraySegment pageContents) {
        // Check ID match.
        int headerId = BitConverter.readInt(pageContents, ID_OFFSET);
        int footerId = BitConverter.readInt(pageContents, pageContents.getLength() - FOOTER_LENGTH);
        if (headerId != footerId) {
            throw new IllegalDataFormatException("Invalid Page Format (id mismatch). HeaderId=%s, FooterId=%s.", headerId, footerId);
        }

        int flags = pageContents.get(FLAGS_OFFSET);
        return (flags & FLAG_INDEX_PAGE) == FLAG_INDEX_PAGE;
    }

    /**
     * Gets a value representing the number of bytes in this BTreePage (header and footer included).
     *
     * @return The number of bytes.
     */
    int getLength() {
        return this.contents.getLength();
    }

    /**
     * Gets a ByteArraySegment representing the value at the given Position.
     *
     * @param pos The position to get the value at.
     * @return A ByteArraySegment containing the value at the given Position. Note that this is a view inside a larger array
     * and any modifications to that array will be reflected in this. If this value needs to be held for
     * longer then it is recommended to get a copy of it (use getCopy()).
     */
    ByteArraySegment getValueAt(int pos) {
        Preconditions.checkElementIndex(pos, getCount(), "pos must be non-negative and smaller than the number of items.");
        return this.data.subSegment(pos * this.config.entryLength + this.config.keyLength, this.config.valueLength);
    }

    /**
     * Gets the Key at the given Position.
     *
     * @param pos The Position to get the Key at.
     * @return A ByteArraySegment containing the Key at the given Position. Note that this is a view inside a larger array
     * and any modifications to that array will be reflected in this. If this value needs to be held for
     * longer then it is recommended to get a copy of it (use getCopy()).
     */
    ByteArraySegment getKeyAt(int pos) {
        Preconditions.checkElementIndex(pos, getCount(), "pos must be non-negative and smaller than the number of items.");
        return this.data.subSegment(pos * this.config.entryLength, this.config.keyLength);
    }

    /**
     * Updates the first PageEntry's key to the given value.
     *
     * @param newKey A ByteArraySegment representing the replacement value for the first key. This must be smaller than
     *               or equal to the current value of the first key (using KEY_COMPARATOR).
     */
    void setFirstKey(ByteArraySegment newKey) {
        Preconditions.checkState(getCount() > 0, "BTreePage is empty. Cannot set first key.");
        Preconditions.checkArgument(newKey.getLength() == this.config.getKeyLength(), "Incorrect key length.");
        Preconditions.checkArgument(KEY_COMPARATOR.compare(newKey, getKeyAt(0)) <= 0,
                "Replacement first Key must be smaller than or equal to the existing first key.");

        this.data.copyFrom(newKey, 0, newKey.getLength());
    }

    /**
     * Gets a PageEntry representing the entry (Key + Value) at the given Position.
     *
     * @param pos The position to get the value at.
     * @return A PageEntry containing the entry at the given Position. Note that the ByteArraySegments returned by this
     * PageEntry's getKey() and getValue() are views inside a larger array and any modifications to that array will be
     * reflected in them. If this value needs to be held for longer then it is recommended to get a copy of it (use getCopy()).
     */
    PageEntry getEntryAt(int pos) {
        Preconditions.checkElementIndex(pos, getCount(), "pos must be non-negative and smaller than the number of items.");
        return new PageEntry(
                this.data.subSegment(pos * this.config.entryLength, this.config.keyLength),
                this.data.subSegment(pos * this.config.entryLength + this.config.keyLength, this.config.valueLength));
    }

    /**
     * Gets all the Page Entries between the two indices.
     *
     * @param firstIndex The first index to get Page Entries from (inclusive).
     * @param lastIndex  The last index to get Page Entries to (inclusive).
     * @return A new List containing the desired result.
     */
    List<PageEntry> getEntries(int firstIndex, int lastIndex) {
        Preconditions.checkArgument(firstIndex <= lastIndex, "firstIndex must be smaller than or equal to lastIndex.");
        ArrayList<PageEntry> result = new ArrayList<>();
        for (int i = firstIndex; i <= lastIndex; i++) {
            result.add(getEntryAt(i));
        }

        return result;
    }

    /**
     * If necessary, splits the contents of this BTreePage instance into multiple BTreePages. This instance will not be
     * modified as a result of this operation (all new BTreePages will be copies).
     *
     * The resulting pages will be about half full each, and when combined in order, they will contain the same elements
     * as this BTreePage, in the same order.
     *
     * Split Conditions:
     * * Length > MaxPageSize
     *
     * @return If a split is made, an ordered List of BTreePage instances. If no split is necessary (condition is not met),
     * returns null.
     */
    List<BTreePage> splitIfNecessary() {
        if (this.contents.getLength() <= this.config.getMaxPageSize()) {
            // Nothing to do.
            return null;
        }

        // Calculate how many pages to split into. While doing so, take care to account that we may only have whole entries
        // in each page, and not partial ones.
        int maxDataLength = (this.config.getMaxPageSize() - this.header.getLength() - this.footer.getLength()) / this.config.entryLength * this.config.entryLength;
        int remainingPageCount = (int) Math.ceil((double) this.data.getLength() / maxDataLength);

        ArrayList<BTreePage> result = new ArrayList<>(remainingPageCount);
        int readIndex = 0;
        int remainingItems = getCount();
        while (remainingPageCount > 0) {
            // Calculate how many items to include in this split page. This is the average of the remaining items over
            // the remaining page count (this helps smooth out cases when the original getCount() is not divisible by
            // the calculated number of splits).
            int itemsPerPage = remainingItems / remainingPageCount;

            // Copy data over to the new page.
            ByteArraySegment splitPageData = this.data.subSegment(readIndex, itemsPerPage * this.config.entryLength);
            result.add(new BTreePage(this.config, itemsPerPage, splitPageData));

            // Update pointers.
            readIndex += splitPageData.getLength();
            remainingPageCount--;
            remainingItems -= itemsPerPage;
        }

        assert readIndex == this.data.getLength() : "did not copy everything";
        return result;
    }

    /**
     * Updates the contents of this BTreePage with the given entries. Entries whose keys already exist will update the data,
     * while Entries whose keys do not already exist will be inserted.
     *
     * After this method completes, this BTreePage:
     * * May overflow (a split may be necessary)
     * * Will have all entries sorted by Key
     *
     * @param entries The Entries to insert or update. This collection need not be sorted.
     */
    void update(@NonNull Collection<PageEntry> entries) {
        if (entries.isEmpty()) {
            // Nothing to do.
            return;
        }

        // Apply the in-place updates and collect the new entries to be added.
        val newEntries = applyUpdates(entries);
        if (newEntries.isEmpty()) {
            // Nothing else to change. We've already updated the keys in-place.
            return;
        }

        val newPage = insertNewEntries(newEntries);

        // Make sure we swap all the segments with those from the new page. We need to release all pointers to our
        // existing buffers.
        this.header = newPage.header;
        this.data = newPage.data;
        this.contents = newPage.contents;
        this.footer = newPage.footer;
        this.count = newPage.count;
    }

    /**
     * Updates the contents of this BTreePage so that it does not contain any entry with the given Keys anymore.
     *
     * After this method completes, this BTreePage:
     * * May underflow (a merge may be necessary)
     * * Will have all entries sorted by Key
     * * Will reuse the same underlying buffer as before (no new buffers allocated). As such it may underutilize the buffer.
     *
     * @param keys A Collection of Keys to remove. The Keys need not be sorted.
     */
    void delete(@NonNull Collection<ByteArraySegment> keys) {
        if (keys.isEmpty()) {
            // Nothing to do.
            return;
        }

        // Locate the positions of the Keys to remove.
        val removedPositions = searchPositions(keys);
        if (removedPositions.size() > 0) {
            // Remove them.
            removePositions(removedPositions);
        }
    }

    /**
     * Gets a ByteArraySegment representing the value mapped to the given Key.
     *
     * @param key The Key to search.
     * @return A ByteArraySegment mapped to the given Key, or null if the Key does not exist. Note that this is a view
     * inside a larger array and any modifications to that array will be reflected in this. If this value needs to be held
     * for longer then it is recommended to get a copy of it (use getCopy()).
     */
    ByteArraySegment searchExact(@NonNull ByteArraySegment key) {
        val pos = search(key, 0);
        if (!pos.isExactMatch()) {
            // Nothing found.
            return null;
        }

        return getValueAt(pos.getPosition());
    }

    /**
     * Performs a (binary) search for the given Key in this BTreePage and returns its position.
     *
     * @param key      A ByteArraySegment that represents the key to search.
     * @param startPos The starting position (not array index) to begin the search at. Any positions prior to this one
     *                 will be ignored.
     * @return A SearchResult instance with the result of the search.
     */
    SearchResult search(@NonNull ByteArraySegment key, int startPos) {
        // Positions here are not indices into "source", rather they are entry positions, which is why we always need
        // to adjust by using entryLength.
        int endPos = getCount();
        Preconditions.checkArgument(startPos <= endPos, "startPos must be non-negative and smaller than the number of items.");
        while (startPos < endPos) {
            // Locate the Key in the middle.
            int midPos = startPos + (endPos - startPos) / 2;

            // Compare it to the sought key.
            int c = KEY_COMPARATOR.compare(key.array(), key.arrayOffset(),
                    this.data.array(), this.data.arrayOffset() + midPos * this.config.entryLength, this.config.keyLength);
            if (c == 0) {
                // Exact match.
                return new SearchResult(midPos, true);
            } else if (c < 0) {
                // Search again to the left.
                endPos = midPos;
            } else {
                // Search again to the right.
                startPos = midPos + 1;
            }
        }

        // Return an inexact search result with the position where the sought key would have been.
        return new SearchResult(startPos, false);
    }

    /**
     * Gets a list of positions where the given Keys exist.
     *
     * @param keys A Collection of ByteArraySegment instances representing the Keys to search.
     * @return A sorted List of Positions representing locations in this BTreePage where the given Keys exist. Keys that
     * do not exist in this BTreePage will not be included.
     */
    private List<Integer> searchPositions(Collection<ByteArraySegment> keys) {
        int maxCount = getCount();
        int lastPos = 0;
        val positions = new ArrayList<Integer>();
        val keyIterator = keys.stream().sorted(KEY_COMPARATOR::compare).iterator();
        while (keyIterator.hasNext() && positions.size() < maxCount) {
            val key = keyIterator.next();
            if (key.getLength() != this.config.keyLength) {
                throw new IllegalDataFormatException("Found a key with unexpected length.");
            }

            val sr = search(key, lastPos);
            if (!sr.exactMatch) {
                // Key does not exist.
                continue;
            }

            positions.add(sr.getPosition());
            lastPos = sr.getPosition() + 1;
        }

        return positions;
    }

    /**
     * Removes the given positions from this BTreePage.
     *
     * @param removedPositions A Sorted List of Positions to remove.
     */
    private void removePositions(List<Integer> removedPositions) {
        // Remember the new count now, before we mess around with things.
        int initialCount = getCount();
        int newCount = initialCount - removedPositions.size();

        // Trim away the data buffer, move the footer back and trim the contents buffer.
        int prevRemovedPos = removedPositions.get(0);
        int writeIndex = prevRemovedPos * this.config.entryLength;
        removedPositions.add(initialCount); // Add a sentinel at the end to make this easier.
        for (int i = 1; i < removedPositions.size(); i++) {
            int removedPos = removedPositions.get(i);
            assert removedPos > prevRemovedPos : "removedPositions is not sorted";
            int readIndex = (prevRemovedPos + 1) * this.config.entryLength;
            int readLength = removedPos * this.config.entryLength - readIndex;
            prevRemovedPos = removedPos;
            if (readLength == 0) {
                // Nothing to do now.
                continue;
            }

            // Copy the data.
            this.data.copyFrom(this.data, readIndex, writeIndex, readLength);
            writeIndex += readLength;
        }

        // Trim away the data buffer, move the footer back and trim the contents buffer.
        assert writeIndex == (initialCount - removedPositions.size() + 1) * this.config.entryLength : "unexpected number of bytes remaining";
        shrink(newCount);
    }

    /**
     * Updates (in-place) the contents of this BTreePage with the given entries for those Keys that already exist. For
     * all the new Keys, collects them into a List and calculates the offset where they would have to be inserted.
     *
     * @param entries A Collection of PageEntries to update.
     * @return A sorted List of Map.Entry instances (Offset -> PageEntry) indicating the new PageEntry instances to insert
     * and at which offset.
     */
    private List<Map.Entry<Integer, PageEntry>> applyUpdates(Collection<PageEntry> entries) {
        // Keep track of new keys to be added along with the offset (in the original page) where they would have belonged.
        val newEntries = new ArrayList<Map.Entry<Integer, PageEntry>>();

        // Process all the Entries, in order (by Key).
        int lastPos = 0;
        val entryIterator = entries.stream().sorted((e1, e2) -> KEY_COMPARATOR.compare(e1.getKey(), e2.getKey())).iterator();
        while (entryIterator.hasNext()) {
            val e = entryIterator.next();
            if (e.getKey().getLength() != this.config.keyLength || e.getValue().getLength() != this.config.valueLength) {
                throw new IllegalDataFormatException("Found an entry with unexpected Key or Value length.");
            }

            // Figure out if this entry exists already.
            val searchResult = search(e.getKey(), lastPos);
            if (searchResult.isExactMatch()) {
                // Keys already exists: update in-place.
                setValueAtPosition(searchResult.getPosition(), e.getValue());
            } else {
                // This entry's key does not exist. We need to remember it for later. Since this was not an exact match,
                // binary search returned the position where it should have been.
                int dataIndex = searchResult.getPosition() * this.config.entryLength;
                newEntries.add(new AbstractMap.SimpleImmutableEntry<>(dataIndex, e));
            }

            // Remember the last position so we may resume the next search from there.
            lastPos = searchResult.position;
        }

        return newEntries;
    }

    /**
     * Inserts the new PageEntry instances at the given offsets.
     *
     * @param newEntries A sorted List of Map.Entry instances (Offset -> PageEntry) indicating the new PageEntry instances
     *                   to insert and at which offset.
     * @return A new BTreePage instance with the updated contents.
     */
    private BTreePage insertNewEntries(List<Map.Entry<Integer, PageEntry>> newEntries) {
        int newCount = getCount() + newEntries.size();

        // If we have extra entries: allocate new buffer of the correct size and start copying from the old one.
        // We cannot reuse the existing buffer because we need more space.
        val newPage = new BTreePage(this.config, new ByteArraySegment(new byte[DATA_OFFSET + newCount * this.config.entryLength + FOOTER_LENGTH]), false);
        newPage.formatHeaderAndFooter(newCount, getHeaderId());
        int readIndex = 0;
        int writeIndex = 0;
        for (val e : newEntries) {
            int entryIndex = e.getKey();
            if (entryIndex > readIndex) {
                // Copy from source.
                int length = entryIndex - readIndex;
                assert length % this.config.entryLength == 0;
                newPage.data.copyFrom(this.data, readIndex, writeIndex, length);
                writeIndex += length;
            }

            // Write new Entry.
            PageEntry entryContents = e.getValue();
            newPage.setEntryAtIndex(writeIndex, entryContents);
            writeIndex += this.config.entryLength;
            readIndex = entryIndex;
        }

        if (readIndex < this.data.getLength()) {
            // Copy the last part that we may have missed.
            int length = this.data.getLength() - readIndex;
            newPage.data.copyFrom(this.data, readIndex, writeIndex, length);
        }

        return newPage;
    }

    /**
     * Shrinks this BTreePage to only contain the given number of elements.
     *
     * @param itemCount The new number of items in this BTreePage.
     */
    private void shrink(int itemCount) {
        Preconditions.checkArgument(itemCount >= 0 && itemCount <= getCount(), "itemCount must be non-negative and at most the current element count");
        int dataLength = itemCount * this.config.entryLength;
        this.data = new ByteArraySegment(this.contents.array(), this.data.arrayOffset(), dataLength);
        this.footer = new ByteArraySegment(this.contents.array(), this.data.arrayOffset() + this.data.getLength(), FOOTER_LENGTH);
        this.contents = new ByteArraySegment(this.contents.array(), this.contents.arrayOffset(), this.footer.arrayOffset() + this.footer.getLength());
        setCount(itemCount);
        setFooterId(getHeaderId());
    }

    /**
     * Updates the Header of this BTreePage to reflect that it contains the given number of items. This does not perform
     * any resizing.
     *
     * @param itemCount The count to set.
     */
    private void setCount(int itemCount) {
        BitConverter.writeInt(this.header, COUNT_OFFSET, itemCount);
        this.count = itemCount;
    }

    /**
     * Sets the Value at the given position.
     *
     * @param pos   The Position to set the value at.
     * @param value A ByteArraySegment representing the value to set.
     */
    private void setValueAtPosition(int pos, ByteArraySegment value) {
        Preconditions.checkElementIndex(pos, getCount(), "pos must be non-negative and smaller than the number of items.");
        Preconditions.checkArgument(value.getLength() == this.config.valueLength, "Given value has incorrect length.");
        this.data.copyFrom(value, pos * this.config.entryLength + this.config.keyLength, value.getLength());
    }

    private void setEntryAtIndex(int dataIndex, PageEntry entry) {
        Preconditions.checkElementIndex(dataIndex, this.data.getLength(), "dataIndex must be non-negative and smaller than the size of the data.");
        Preconditions.checkArgument(entry.getKey().getLength() == this.config.keyLength, "Given entry key has incorrect length.");
        Preconditions.checkArgument(entry.getValue().getLength() == this.config.valueLength, "Given entry value has incorrect length.");

        this.data.copyFrom(entry.getKey(), dataIndex, entry.getKey().getLength());
        this.data.copyFrom(entry.getValue(), dataIndex + this.config.keyLength, entry.getValue().getLength());
    }

    private byte getFlags(byte... flags) {
        byte result = 0;
        for (byte f : flags) {
            result |= f;
        }
        return result;
    }

    /**
     * Gets this BTreePage's Id from its header.
     */
    int getHeaderId() {
        return BitConverter.readInt(this.header, ID_OFFSET);
    }

    /**
     * Gets this BTreePage's Id from its footer.
     */
    private int getFooterId() {
        return BitConverter.readInt(this.footer, 0);
    }

    /**
     * Updates the Header to contain the given id.
     */
    private void setHeaderId(int id) {
        BitConverter.writeInt(this.header, ID_OFFSET, id);
    }

    /**
     * Updates the Footer to contain the given id.
     */
    private void setFooterId(int id) {
        BitConverter.writeInt(this.footer, 0, id);
    }

    //endregion

    //region Config

    /**
     * BTreePage Configuration.
     */
    @RequiredArgsConstructor
    @Getter
    static class Config {
        /**
         * The length, in bytes, for all Keys.
         */
        private final int keyLength;
        /**
         * The length, in bytes, for all Values.
         */
        private final int valueLength;
        /**
         * The length, in bytes, for all Entries (KeyLength + ValueLength).
         */
        private final int entryLength;
        /**
         * Maximum length, in bytes, of any BTreePage (including header, data and footer).
         */
        private final int maxPageSize;
        /**
         * Whether this is an Index Page or not.
         */
        private final boolean isIndexPage;

        /**
         * Creates a new instance of the BTreePage.Config class.
         *
         * @param keyLength   The length, in bytes, of all Keys.
         * @param valueLength The length, in bytes, of all Values.
         * @param maxPageSize Maximum length, in bytes, of any BTreePage.
         * @param isIndexPage Whether this is an Index Page or not.
         */
        Config(int keyLength, int valueLength, int maxPageSize, boolean isIndexPage) {
            Preconditions.checkArgument(keyLength > 0, "keyLength must be a positive integer.");
            Preconditions.checkArgument(valueLength > 0, "valueLength must be a positive integer.");
            Preconditions.checkArgument(keyLength + valueLength + DATA_OFFSET + FOOTER_LENGTH <= maxPageSize,
                    "maxPageSize must be able to fit at least one entry.");
            this.keyLength = keyLength;
            this.valueLength = valueLength;
            this.entryLength = this.keyLength + this.valueLength;
            this.maxPageSize = maxPageSize;
            this.isIndexPage = isIndexPage;
        }
    }

    //endregion

    //region SearchResult

    /**
     * The result of a BTreePage Search.
     */
    @Getter
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    static class SearchResult {
        /**
         * The resulting position.
         */
        private final int position;
        /**
         * Indicates whether an exact match for the sought key was found. If so, position refers to that key. If not,
         * position refers to the location where the key would have been.
         */
        private final boolean exactMatch;

        @Override
        public String toString() {
            return String.format("%s (%s)", this.position, this.exactMatch ? "E" : "NE");
        }
    }

    //endregion
}
