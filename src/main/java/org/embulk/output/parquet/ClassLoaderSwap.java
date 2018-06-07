package org.embulk.output.parquet;

/**
 * This class is based on embulk-input-parquet_hadoop PluginClassLoaderScope.java
 */
public class ClassLoaderSwap<T> implements AutoCloseable
{
    private final ClassLoader pluginClassLoader;
    private final ClassLoader orgClassLoader;
    private final Thread curThread;

    public ClassLoaderSwap(Class<T> pluginClass)
    {
        this.curThread = Thread.currentThread();
        this.pluginClassLoader = pluginClass.getClassLoader();
        this.orgClassLoader = curThread.getContextClassLoader();
        curThread.setContextClassLoader(pluginClassLoader);
    }

    @Override
    public void close()
    {
        curThread.setContextClassLoader(orgClassLoader);
    }
}
