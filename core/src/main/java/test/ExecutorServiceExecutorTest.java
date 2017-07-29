package test; /**
 * Created by liush on 17-7-29.
 */
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExecutorServiceExecutorTest {
    public static void main(String[] args) {
//                ExecutorService executorService = Executors.newCachedThreadPool();
        //创建一个ExecutorService的实例,ExecutorService实际上是一个线程池的管理工具
        ExecutorService executorService = Executors.newFixedThreadPool(5);
//         ExecutorService executorService = Executors.newSingleThreadExecutor();

        for (int i = 0; i < 5; i++) {
            //将任务添加到线程去执行
            executorService.execute(new TestRunnable());
            System.out.println("************* a" + i + " *************");
        }
        /**
         * 获取任务的执行的返回值:
         * 任务分两类：一类是实现了Runnable接口的类，一类是实现了Callable接口的类。
         * 两者都可以被ExecutorService执行,但是Runnable任务没有返回值,而Callable任务有返回值。
         * Callable中的call()方法类似Runnable的run()方法，就是前者有返回值，后者没有。
         *
         * 当将一个Callable的对象传递给ExecutorService的submit方法，则该call方法自动在一个线程上执行，并且会返回执行结果Future对象。
         * 同样，将Runnable的对象传递给ExecutorService的submit方法，则该run方法自动在一个线程上执行，并且会返回执行结果Future对象，
         * 但是在该Future对象上调用get方法，将返回null。
         */
        //关闭执行服务对象
        executorService.shutdown();
    }


}

class TestRunnable implements Runnable {
    //获取任务的执行的返回值:Runnable任务没有返回值
    public void run() {
        System.out.println(Thread.currentThread().getName() + "线程被调用了。");
        //死循环打印线程名称
        while (true) {
            try {
                Thread.sleep(5000);
                System.out.println(Thread.currentThread().getName());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}