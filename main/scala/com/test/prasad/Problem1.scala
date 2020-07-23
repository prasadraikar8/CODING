package com.test.prasad

import java.io.{BufferedWriter, File, FileInputStream, FileWriter}

import org.apache.poi.ss.usermodel.{Cell, CellType, Row}
import org.apache.poi.xssf.usermodel.{XSSFSheet, XSSFWorkbook}

import scala.collection.mutable
import scala.util.control.Breaks._
import scala.collection.mutable.Map
import scala.collection.mutable.HashSet;

object Problem1 {

  def main(args: Array[String]): Unit = {

    try {
      val file = new FileInputStream(new File("D:\\\\InternationalBaseline2019-Final.xlsx"));
      val workbook = new XSSFWorkbook(file);
     val Array_sheets=Array("Barley","Beef","Corn","Cotton","Pork","Poultry","Rice","Sorghum","Soybeans","Soybean meal","Soybean oil","Wheat");
  // val Array_sheets=Array("Barley","Beef");
      val Map:mutable.HashMap[String,Option[Double]]= mutable.HashMap[String,Option[Double]]();
      val hashSet: HashSet[String]=HashSet[String]();


      Array_sheets.foreach(elem=> {
        System.out.println(elem);

        val sheet = workbook.getSheet(elem);

        val rowIterator = sheet.iterator();
        var usa_flag = 0;
        var world_flag = 0;
        while (rowIterator.hasNext()) {
          val row = rowIterator.next();
          var startRow = 0;
          var counter = 1;

          var key = "";
          var year = "";
          var good=0;
          val cellIterator = row.cellIterator();

          var value: Option[Double] =None;


          breakable {
            while (cellIterator.hasNext()) {
              val cell = cellIterator.next();

              if (counter == 1 && cell.getCellType.equals(CellType.STRING) && cell.getStringCellValue.startsWith("USA")) {
                usa_flag = 1;

                break;
              }
              if (counter == 1 && cell.getCellType.equals(CellType.STRING) && cell.getStringCellValue.startsWith("WORLD")) {
                world_flag = 1
                System.out.print("set world flag");
                break
              }

              if (counter == 1 && cell.getCellType.equals(CellType.BLANK) && usa_flag == 1) {
                usa_flag = 0;
                break;
              }

              if (counter == 1 && cell.getCellType.equals(CellType.BLANK) && world_flag == 1) {
                world_flag = 0;
                //System.out.print("reset world flag");
                break;
              }

             if (counter==1 && (cell.getCellType.equals(CellType.NUMERIC)||
                (cell.getCellType.equals(CellType.STRING)&&(cell.getStringCellValue.trim.startsWith("20"))
                  ))) {
               good=1; }

             if(good==0) break;

              value = None
              if ((usa_flag == 1 || world_flag == 1 )&& good==1) {

                  (counter, cell.getCellType) match {
                    case (1, CellType.NUMERIC) => year = cell.getNumericCellValue.toInt.toString + "/" + (cell.getNumericCellValue.toString.substring(2, 4).toInt+1)

                    case (1, CellType.STRING) => year = cell.getStringCellValue.trim()

                    case _ => ""

                  }


                hashSet.add(year)
                  counter match{
                    case 1=>   usa_flag match { case 1=> key = "USA_" + elem.replaceAll(" ", "_").toUpperCase + "_" + year

                    case _ =>}

                      world_flag match { case 1=> key = "WORLD_" + elem.replaceAll(" ", "_").toUpperCase + "_" + year

                      case _ =>}


                    case 2 =>
                               if(cell.getCellType.equals(CellType.NUMERIC))
                               {
                                 value = Option(cell.getNumericCellValue())

                               if(!Map.contains(key))  Map.put(key, value)}
                      if(cell.getCellType.equals(CellType.STRING) && cell.getStringCellValue.trim.equals("--"))
                      {
                        if(!Map.contains(key))  Map.put(key, None)}


                    case _ =>



                  }

                  counter = counter + 1;



              }
            }


          }
        }
      })


      System.out.println("outside  ")
      Map.foreach(x=> println(x))

      System.out.println("imp  " + Map.get("WORLD_CORN_2019/20"))
      System.out.println("imp  " + Map.get("USA_CORN_2019/20"))
      file.close();


     writeFile("D:\\OUTPUT_FILE.csv",hashSet,Map,Array_sheets)

      //Outputng to a file
    }
    catch {
      case e:Exception =>
      println(e.printStackTrace())}


  }

  def writeFile(filename: String,    hashSet: HashSet[String],hashMap: mutable.HashMap[String,Option[Double]],Array_sheets:Array[String] ): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    val sb = new StringBuilder
    bw.write("Year|world_barley_harvest|usa_barley_contribution%|world_beef_harvest|usa_beef_contribution%|world_corn_harvest|usa_corn_contribution%|world_cotton_harvest|usa_cotton_contribution%|world_pork_harvest|usa_pork_contribution%|world_poultry_harvest|usa_poultry_contribution%|world_rice_harvest|usa_rice_contribution%|world_sorghum_harvest|usa_sorghum_contribution%|world_soybeans_harvest|usa_soybeans_contribution%|world_soybean_meal_harvest|usa_soybean_meal_contribution%|world_soybean_oil_harvest|usa_soybean_oil_contribution%|world_wheat_harvest|usa_wheat_contribution%\n")

   hashSet.foreach(x=>

     {
       sb.append(x)
       Array_sheets.foreach(
         sheet=>{
           sb.append("|")
           sb.append(hashMap.get("WORLD_" + sheet.replaceAll(" ", "_").toUpperCase + "_" + x).get.get.toInt)
           sb.append("|")
           sb.append( (hashMap.get("USA_" + sheet.replaceAll(" ", "_").toUpperCase + "_" + x).get.getOrElse(0.0)/hashMap.get("WORLD_" + sheet.replaceAll(" ", "_").toUpperCase + "_" + x).get.get)*100)



         }


       )
       sb.append("\n")







     }




   )

    bw.write(sb.toString())

    bw.close()
  }

}
