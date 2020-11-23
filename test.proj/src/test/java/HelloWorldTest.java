import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class HelloWorldTest {
    @Test
    public void Test2(){
        HelloWorld h = new HelloWorld();
        assertEquals(h.returnInt(), 1);
    }
}
