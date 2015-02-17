Embulk::JavaPlugin.register_output(
  :parquet, "org.embulk.output.ParquetOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
