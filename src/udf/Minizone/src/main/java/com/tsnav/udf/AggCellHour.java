package com.tsnav.udf;

import com.aliyun.odps.io.Writable;

import java.util.ArrayList;
import java.util.Collections;

import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.udf.Aggregator;
import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.annotation.Resolve;

@Resolve({"bigint,bigint->string"})
public class AggCellHour extends Aggregator {

  public void setup(ExecutionContext ctx) throws UDFException {
  }

  public Writable newBuffer() {
    return new Text();
  }

  public void iterate(Writable buffer, Writable[] args) throws UDFException {
    Text result = (Text) buffer;
    LongWritable cell_id = (LongWritable) args[0];
    LongWritable hour = (LongWritable) args[1];
    result.set(result + cell_id.toString() + "@" + hour.toString() +"|");
  }

  public void merge(Writable buffer, Writable part) throws UDFException {
    Text result = (Text) buffer;
    Text partial = (Text) part;
    result.set("" + result + partial);
  }
  
  public String getResult(String raw){
    String result = "";
    if(raw.length()>1){
      raw = raw.substring(0, raw.length()-1);
      String pairs[] = raw.split("\\|");
      ArrayList<ArrayList<Integer>> hour_cell_map = new ArrayList<ArrayList<Integer>>(24);
      for(int i=0; i<24; i++){
        hour_cell_map.add(i, null);
      }
      for(String pair : pairs){
        String[] cell_hour = pair.split("@");
        if(cell_hour.length==2){
          int cell = Integer.parseInt(cell_hour[0]);
          int hour = Integer.parseInt(cell_hour[1]);
          if(hour_cell_map.get(hour) != null){
            ArrayList<Integer> cell_list = hour_cell_map.get(hour);
            cell_list.add(cell);
            hour_cell_map.set(hour, cell_list);
          }else{
            ArrayList<Integer> cell_list = new ArrayList<Integer>();
            cell_list.add(cell);
            hour_cell_map.set(hour, cell_list);
          }
        }
      }
      int begin_hour = -1;
      int last_hour = -1;
      int last_cell_id = -1;
      int duration = -1;
      boolean is_first_cell = true;
      for(int i=0; i<24; i++){
        int current_hour = i;
        int max_count_cell_id = -1;
        ArrayList<Integer> cell_list = hour_cell_map.get(i);
        if(cell_list != null){//fixed here
          int max_count = -1;
          for(int cell_id : cell_list){//to set first
            int cell_count = Collections.frequency(cell_list, cell_id);
            if (cell_count > max_count){
              max_count = cell_count;
              max_count_cell_id = cell_id;
            }
          }
          int current_cell_id = max_count_cell_id;
          if(is_first_cell){
            is_first_cell = false;
            last_cell_id = current_cell_id;
            begin_hour = i;
            last_hour = i;
            duration=1;
          } else if(current_cell_id != last_cell_id || 
                    (current_hour-1) != last_hour){
            result += last_cell_id+"@"+begin_hour+"#"+duration+"|";
            duration = 1;
            last_cell_id = current_cell_id;
            begin_hour = i;
            last_hour = i;
          } else if(current_cell_id == last_cell_id &&
                    (current_hour-1) == last_hour){
            duration++;
            last_hour = i;
          }
        }
      }
      result += last_cell_id+"@"+begin_hour+"#"+duration;
    }
    return result;
  }

  public Writable terminate(Writable buffer) throws UDFException {
    return new Text(getResult(((Text) buffer).toString()));
  }

  public void close() throws UDFException {
  }
  
  public static void main(String[] args) {
    //String raw = "14319@15|14319@5|14319@7|14319@1|14319@4|14319@2|14319@3|14319@10|14319@20|14319@12|14319@22|14319@14|14319@23|14319@16|14319@13|";
    String raw="1111@0|7777@0|7777@0|1111@1|1111@2|2222@3|3333@4|3333@5|2222@0|8888@10|9999@15|";
    AggCellHour test = new AggCellHour();
    String result = test.getResult(raw);
    System.out.println("result:"+result);

  }
}