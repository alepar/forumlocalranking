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

/*
Prerequisites:
 - Get a Hadoop binary installation (3.3.0), put the location into HADOOP_HOME
     * this is extra fun on Windows - you need to build it yourself or download at one of the locations below:
         + https://github.com/steveloughran/winutils
         + https://github.com/cdarlint/winutils
 - Run your JVM with the following option: -Djava.library.path=<path to yor libhadoop.so/hadoop.dll>
   Usually this is not necessary, it will just warn/complain about this missing.

 - If you want to have it in your own project, additionally you'll need:
     * following maven deps (see local pom).
         + parquet-avro
         + hadoop-common
         + hadoop-mapreduce-client-core (only if reading parquet)
     * set up avro-maven-plugin to generate the model classes from the avro schema
     * note that parquet-avro is linked with specific versions of hadoop and avro, so ideally it all should match throughout
     * same goes for hadoop jars and hadoop binaries
     * it is not super picky about version matching, so as long as it is "close enough" you should be good
*/

fun main() {
    createParqueWriter("test.parquet", User::class).use { writer ->
        writer.write(User("test", "M", 10, -1))
        writer.write(User("test2", "F", 999, 123))
    }

    openParqueReader("test.parquet", User::class).use { reader ->
        var user = reader.read()
        do {
            println(user)
            user = reader.read()
        } while(user != null)
    }
}

fun <T : GenericContainer> createParqueWriter(filePath: String, klass: KClass<T>): ParquetWriter<T> {
    return AvroParquetWriter.builder<T>(Path(filePath))
            .withSchema(getSchemaFromModelClass(klass.java))
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .build()
}

fun <T : GenericContainer> openParqueReader(filePath: String, klass: KClass<T>): ParquetReader<T> {
    val cfg = Configuration()
    AvroReadSupport.setRequestedProjection(cfg, getSchemaFromModelClass(klass.java))

    return AvroParquetReader.builder<T>(HadoopInputFile.fromPath(Path(filePath), cfg))
            .withConf(cfg)
            .build()
}

fun getSchemaFromModelClass(javaClass: Class<*>): Schema {
    val method = javaClass.getDeclaredMethod("getClassSchema")
    val obj = method.invoke(null)
    return obj as Schema
}
