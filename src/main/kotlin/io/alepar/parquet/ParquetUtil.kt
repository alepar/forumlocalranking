package io.alepar.parquet

import org.apache.avro.Schema
import org.apache.avro.generic.GenericContainer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.avro.AvroReadSupport
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.HadoopInputFile
import kotlin.reflect.KClass

fun <T : GenericContainer> createParquetFile(filePath: String, klass: KClass<T>): ParquetWriter<T> {
    return AvroParquetWriter.builder<T>(Path(filePath))
            .withSchema(getSchemaFromModelClass(klass.java))
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .build()
}

fun <T : GenericContainer> readParquetFile(filePath: String, klass: KClass<T>): ParquetReader<T> {
    val cfg = Configuration()
    AvroReadSupport.setRequestedProjection(cfg, getSchemaFromModelClass(klass.java))

    return AvroParquetReader.builder<T>(HadoopInputFile.fromPath(Path(filePath), cfg))
            .withConf(cfg)
            .build()
}

private fun getSchemaFromModelClass(javaClass: Class<*>): Schema {
    val method = javaClass.getDeclaredMethod("getClassSchema")
    val obj = method.invoke(null)
    return obj as Schema
}