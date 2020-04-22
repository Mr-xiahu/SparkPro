import com.alibaba.fastjson.JSON

object Test2 {
  def main(args: Array[String]): Unit = {
    val json = "{\"jobid\":\"1\",\"table\":\"ods.t1\",\"column\":{\"source_vistedno\":\"vistedno\",\"source_hospital\":\"hospital\",\"source_system\":\"system\"},\"targetTable\":\"ods.t1\"}";
    val pojo: Pojo = JSON.parseObject(json, classOf[Pojo])
    println(pojo.getColumn.get("source_vistedno"))

  }
}
