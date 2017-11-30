package test.kafka

import java.io.InputStream
import java.util.Properties

/**
  */
trait GetProperties {
  def inputStreamArray: Array[InputStream]

  lazy val props: Properties = {
    val props = new Properties
    inputStreamArray.foreach( inputStream => {
      props.load(inputStream)
      inputStream.close()
    })

    props
  }
}