import static org.junit.Assert.assertEquals;
import org.junit.Test;

import java.util.ArrayList;

public class BinarySearchTest {
    @Test
    public void Test1(){
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
        assertEquals(b.binarySearch(sortedArr, 7), -1);
    }
}
