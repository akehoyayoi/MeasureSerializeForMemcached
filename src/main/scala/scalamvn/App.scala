package scalamvn

import java.nio.ByteBuffer
import net.rubyeye.xmemcached.{XMemcachedClientBuilder, XMemcachedClient}
import net.rubyeye.xmemcached.utils.AddrUtil

import org.msgpack._
import org.msgpack.annotation.Message
import org.msgpack.ScalaMessagePack._
import com.esotericsoftware.kryo.{ObjectBuffer, Kryo}
import java.io.{OutputStreamWriter, ByteArrayOutputStream, FileOutputStream}


@Message
class User(var id : Long , var name : String)
  extends Serializable {
  def this() = this(0,"")
}

object MeasureApp extends App {

  var fos = new FileOutputStream("output.csv")
  var out = new OutputStreamWriter(fos)

  // initialize
  val builder = new XMemcachedClientBuilder(AddrUtil.getAddresses("localhost:11211"))
  val client = builder.build()

  // kryo initialize
  val kryo : Kryo = new Kryo
  kryo.register(classOf[User])
  kryo.setRegistrationOptional(true)
  val buffer = new ObjectBuffer(kryo)

  out.write("module,serializable,serializable,messagepack,messagepack,kryo,kryo\n")
  out.write("method,set,get,set,get,set,get\n")

  def measurementPerformance(measurementSize : Int , tryCount : Int = 10) = {
    val list1 = (1 to measurementSize).map(p => ("1.%d".format(p),new User(p.toLong,"takeshi%d".format(p))))
    val list2 = (1 to measurementSize).map(p => ("2.%d".format(p),new User(p.toLong,"hiroshi%d".format(p))))
    val list3 = (1 to measurementSize).map(p => ("3.%d".format(p),new User(p.toLong,"takashi%d".format(p))))
    var message = ""

    def measureExecute(f : => Unit) : Long ={
      (1 to tryCount).map(p => {
        var before = System.currentTimeMillis()
        f
        System.currentTimeMillis() - before
      }).sum / tryCount.toLong
    }

    message += (measurementSize + ",")

    // -------------- Serializable measurement --------------
    // -------------- set performance --------------
    message += (measureExecute({
      list1.foreach(p => client.set(p._1,0,p._2))
    }) + ",")

    // -------------- get performance --------------
    message += (measureExecute({
      list1.foreach(p => client.get[User](p._1))
    }) + ",")

    // -------------- messagepack measurement ---------------
    // -------------- set performance --------------
    message += (measureExecute({
      list2.foreach(p => client.set(p._1,0,write(p._2)))
    }) + ",")

    // -------------- get performance --------------
    message += (measureExecute({
      list2.foreach(p => read[User](client.get[Array[Byte]](p._1)))
    }) + ",")

    // -------------- Kryo measurement ----------------------
    // -------------- set performance --------------
    message += (measureExecute({
      list3.foreach(p => {
        val output = new ByteArrayOutputStream
        buffer.writeObject(output,p._2)
        client.set(p._1,0,output.toByteArray)
      })
    }) + ",")

    // -------------- get performance --------------
    message += (measureExecute({
      list3.foreach(p => {
        buffer.readObject[User](client.get[Array[Byte]](p._1),classOf[User])
      })
    }) + "\n")
    out.write(message)
  }

  measurementPerformance(1,1) // for first access

  (0 to 5).foreach(i => {
    measurementPerformance(scala.math.pow(10,i).toInt)
  })

  out.close()
  client.shutdown()


}
