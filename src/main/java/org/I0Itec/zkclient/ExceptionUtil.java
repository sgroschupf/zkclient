package org.I0Itec.zkclient;

public class ExceptionUtil {

    public static RuntimeException convertToRuntimeException(Exception e) {
        if (e instanceof RuntimeException) {
            return (RuntimeException) e;
        }
        retainInterruptFlag(e);
        return new RuntimeException(e);
    }

    /**
     * This sets the interrupt flag if the catched exception was an
     * {@link InterruptedException}. Catching such an exception always clears
     * the interrupt flag.
     * 
     * @param catchedException
     *            The catched exception.
     */
    public static void retainInterruptFlag(Throwable catchedException) {
        if (catchedException instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
    }
}
