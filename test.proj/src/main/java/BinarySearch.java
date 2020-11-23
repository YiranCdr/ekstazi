import java.util.ArrayList;

public class BinarySearch {
    public int binarySearch(ArrayList<Integer> sortedArr, Integer target) {
        if (sortedArr.size() == 0) {
            return -1;
        }

        // check if ascending
        Integer prevVal = sortedArr.get(0);
        for (int i = 1; i < sortedArr.size(); i++) {
            Integer curVal = sortedArr.get(i);
            if (curVal < prevVal) {
                return -2;
            } else {
                prevVal = curVal;
            }
        }


        int lIdx = 0;
        int rIdx = sortedArr.size() - 1;
        while (lIdx <= rIdx) {
            Integer lVal = sortedArr.get(lIdx);
            Integer rVal = sortedArr.get(rIdx);
            int mIdx = (lIdx + rIdx) / 2;
            Integer mVal = sortedArr.get(mIdx);
            if (mVal > target) {
                rIdx = mIdx - 1;
            } else if (mVal < target) {
                lIdx = mIdx + 1;
            } else {
                return mIdx;
            }
        }
        return -1;
    }

    public static void main(String [] args) {
        ArrayList<Integer> sortedArr = new ArrayList<Integer>() {
            {
                add(0);
                add(1);
                add(3);
                add(4);
                add(6);
                add(9);
                add(13);
            }
        };
        BinarySearch b = new BinarySearch();
        System.out.println(b.binarySearch(sortedArr, 3));
        System.out.println(b.binarySearch(sortedArr, 4));
        System.out.println(b.binarySearch(sortedArr, 9));
        System.out.println(b.binarySearch(sortedArr, 13));
        System.out.println(b.binarySearch(sortedArr, 7));
    }
}
