package bplus;

import java.io.Serializable;
import java.util.ArrayList;

public class BPTreeLeafNode<T extends Comparable<T>> extends BPTreeNode<T> implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private ArrayList<Ref>[] records;
	private BPTreeLeafNode<T> next;

	@SuppressWarnings("unchecked")
	public BPTreeLeafNode(int n) {
		super(n);
		keys = new Comparable[n];
		records = new ArrayList[n];

	}

	/**
	 * @return the next leaf node
	 */
	public BPTreeLeafNode<T> getNext() {
		return this.next;
	}

	/**
	 * sets the next leaf node
	 * 
	 * @param node the next leaf node
	 */
	public void setNext(BPTreeLeafNode<T> node) {
		this.next = node;
	}

	/**
	 * @param index the index to find its record
	 * @return the reference of the queried index
	 */
	public ArrayList<Ref> getRecord(int index) {
		return records[index];
	}

	/**
	 * sets the record at the given index with the passed reference
	 * 
	 * @param index           the index to set the value at
	 * @param recordReference the reference to the record
	 */
	public void setRecord(int index, Ref recordReference) {
		if (records[index] == null)
			records[index] = new ArrayList<Ref>();
		records[index].add(recordReference);
	}

	public void setRecord(int index, ArrayList<Ref> recordReferences) {
		if (records[index] == null)
			records[index] = new ArrayList<Ref>();
		for (Ref r : recordReferences)
			records[index].add(r);
	}

	/**
	 * @return the reference of the last record
	 */
	public ArrayList<Ref> getFirstRecord() {
		return records[0];
	}

	/**
	 * @return the reference of the last record
	 */
	public ArrayList<Ref> getLastRecord() {
		return records[numberOfKeys - 1];
	}

	/**
	 * finds the minimum number of keys the current node must hold
	 */
	public int minKeys() {
		if (this.isRoot())
			return 1;
		return (order + 1) / 2;
	}

	/**
	 * insert the specified key associated with a given record refernce in the B+
	 * tree
	 */
	public PushUp<T> insert(T key, Ref recordReference, BPTreeInnerNode<T> parent, int ptr) {
		if (this.isFull() && !this.contains(key)) {
			BPTreeNode<T> newNode = this.split(key, recordReference);
			Comparable<T> newKey = newNode.getFirstKey();
			return new PushUp<T>(newNode, newKey);
		} else {
			int index = 0;
			while (index < numberOfKeys && getKey(index).compareTo(key) <= 0)
				++index;
			this.insertAt(index, key, recordReference);
			return null;
		}
	}

	/**
	 * inserts the passed key associated with its record reference in the specified
	 * index
	 * 
	 * @param index           the index at which the key will be inserted
	 * @param key             the key to be inserted
	 * @param recordReference the pointer to the record associated with the key
	 */
	private void insertAt(int index, Comparable<T> key, Ref recordReference) {
		System.out.println("here for "+key.toString());
		boolean hadtoShift = false;
		for (int i = numberOfKeys - 1; i >= index; --i) {
			if (this.contains(key)) {
				break;
			}
			hadtoShift = true;
			this.setKey(i + 1, getKey(i));
			records[i+1] = records[i];
		}
		boolean flag = false;
		for (int i = 0; i < keys.length; i++)
			if (key.equals(keys[i])) {
				flag = true;
				index = i;
				break;
			}
		this.setKey(index, key);
		if(hadtoShift) {
			records[index] = new ArrayList<Ref>();
			records[index].add(recordReference);
		}
		else
			this.setRecord(index, recordReference);
		if (flag == false)
			++numberOfKeys;
	}

	private void insertAt(int index, Comparable<T> key, ArrayList<Ref> recordReference) {
		for (int i = numberOfKeys - 1; i >= index; --i) {
			this.setKey(i + 1, getKey(i));
			this.setRecord(i + 1, getRecord(i));
		}
		boolean flag = false;
		for (int i = 0; i < keys.length; i++)
			if (key.equals(keys[i])) {
				flag = true;
				index = i;
				break;
			}
		this.setKey(index, key);
		this.setRecord(index, recordReference);
		if (flag == false)
			++numberOfKeys;
	}

	/**
	 * splits the current node
	 * 
	 * @param key             the new key that caused the split
	 * @param recordReference the reference of the new key
	 * @return the new node that results from the split
	 */
	public BPTreeNode<T> split(T key, Ref recordReference) {
		int keyIndex = this.findIndex(key);
		int midIndex = numberOfKeys / 2;
		if ((numberOfKeys & 1) == 1 && keyIndex > midIndex) // split nodes evenly
			++midIndex;

		int totalKeys = numberOfKeys + 1;
		// move keys to a new node
		BPTreeLeafNode<T> newNode = new BPTreeLeafNode<T>(order);
		for (int i = midIndex; i < totalKeys - 1; ++i) {
			newNode.insertAt(i - midIndex, this.getKey(i), this.getRecord(i));
			numberOfKeys--;
		}

		// insert the new key
		if (keyIndex < totalKeys / 2)
			this.insertAt(keyIndex, key, recordReference);
		else
			newNode.insertAt(keyIndex - midIndex, key, recordReference);

		// set next pointers
		newNode.setNext(this.getNext());
		this.setNext(newNode);

		return newNode;
	}

	/**
	 * finds the index at which the passed key must be located
	 * 
	 * @param key the key to be checked for its location
	 * @return the expected index of the key
	 */
	public int findIndex(T key) {
		for (int i = 0; i < numberOfKeys; ++i) {
			int cmp = getKey(i).compareTo(key);
			if (cmp > 0)
				return i;
		}
		return numberOfKeys;
	}

	/**
	 * returns the record reference with the passed key and null if does not exist
	 */
	@Override
	public ArrayList<Ref> search(T key) {
		for (int i = 0; i < numberOfKeys; ++i)
			if (this.getKey(i).compareTo(key) == 0)
				return this.getRecord(i);
		return null;
	}

	/**
	 * delete the passed key from the B+ tree
	 */
	public boolean delete(T key, BPTreeInnerNode<T> parent, int ptr) {
		for (int i = 0; i < numberOfKeys; ++i)
			if (keys[i].compareTo(key) == 0) {
				this.deleteAt(i);
				if (i == 0 && ptr > 0) {
					// update key at parent
					parent.setKey(ptr - 1, this.getFirstKey());
				}
				// check that node has enough keys
				if (!this.isRoot() && numberOfKeys < this.minKeys()) {
					// 1.try to borrow
					if (borrow(parent, ptr))
						return true;
					// 2.merge
					merge(parent, ptr);
				}
				return true;
			}
		return false;
	}
	public boolean delete(T key, BPTreeInnerNode<T> parent, int ptr, String pageNum) {
		for (int i = 0; i < numberOfKeys; ++i)
			if (keys[i].compareTo(key) == 0) {
				this.deleteAt(i, pageNum);
				if (i == 0 && ptr > 0) {
					// update key at parent
					parent.setKey(ptr - 1, this.getFirstKey());
				}
				// check that node has enough keys
				if (!this.isRoot() && numberOfKeys < this.minKeys()) {
					// 1.try to borrow
					if (borrow(parent, ptr))
						return true;
					// 2.merge
					merge(parent, ptr);
				}
				return true;
			}
		return false;
	}

	/**
	 * delete a key at the specified index of the node
	 * 
	 * @param index the index of the key to be deleted
	 */
	public void deleteAt(int index) {
		for (int i = index; i < numberOfKeys - 1; ++i) {
			keys[i] = keys[i + 1];
			records[i] = records[i + 1];
		}
		numberOfKeys--;
	}

	public void deleteAt(int index, String pageNum) {
		for (Ref r : records[index]) {
			if (r.getPage().equals(pageNum)) {
				records[index].remove(r);
				break;
			}
		}
		if (records[index].isEmpty()) {
			for (int i = index; i < numberOfKeys - 1; ++i) {
				keys[i] = keys[i + 1];
				records[i] = records[i + 1];
			}
			numberOfKeys--;
		}
	}

	/**
	 * tries to borrow a key from the left or right sibling
	 * 
	 * @param parent the parent of the current node
	 * @param ptr    the index of the parent pointer that points to this node
	 * @return true if borrow is done successfully and false otherwise
	 */
	public boolean borrow(BPTreeInnerNode<T> parent, int ptr) {
		// check left sibling
		if (ptr > 0) {
			BPTreeLeafNode<T> leftSibling = (BPTreeLeafNode<T>) parent.getChild(ptr - 1);
			if (leftSibling.numberOfKeys > leftSibling.minKeys()) {
				this.insertAt(0, leftSibling.getLastKey(), leftSibling.getLastRecord());
				leftSibling.deleteAt(leftSibling.numberOfKeys - 1);
				parent.setKey(ptr - 1, keys[0]);
				return true;
			}
		}

		// check right sibling
		if (ptr < parent.numberOfKeys) {
			BPTreeLeafNode<T> rightSibling = (BPTreeLeafNode<T>) parent.getChild(ptr + 1);
			if (rightSibling.numberOfKeys > rightSibling.minKeys()) {
				this.insertAt(numberOfKeys, rightSibling.getFirstKey(), rightSibling.getFirstRecord());
				rightSibling.deleteAt(0);
				parent.setKey(ptr, rightSibling.getFirstKey());
				return true;
			}
		}
		return false;
	}

	/**
	 * merges the current node with its left or right sibling
	 * 
	 * @param parent the parent of the current node
	 * @param ptr    the index of the parent pointer that points to this node
	 */
	public void merge(BPTreeInnerNode<T> parent, int ptr) {
		if (ptr > 0) {
			// merge with left
			BPTreeLeafNode<T> leftSibling = (BPTreeLeafNode<T>) parent.getChild(ptr - 1);
			leftSibling.merge(this);
			parent.deleteAt(ptr - 1);
		} else {
			// merge with right
			BPTreeLeafNode<T> rightSibling = (BPTreeLeafNode<T>) parent.getChild(ptr + 1);
			this.merge(rightSibling);
			parent.deleteAt(ptr);
		}
	}

	/**
	 * merge the current node with the specified node. The foreign node will be
	 * deleted
	 * 
	 * @param foreignNode the node to be merged with the current node
	 */
	public void merge(BPTreeLeafNode<T> foreignNode) {
		for (int i = 0; i < foreignNode.numberOfKeys; ++i)
			this.insertAt(numberOfKeys, foreignNode.getKey(i), foreignNode.getRecord(i));

		this.setNext(foreignNode.getNext());
	}
}