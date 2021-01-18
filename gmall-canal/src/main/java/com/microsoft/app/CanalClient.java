package com.microsoft.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.microsoft.constants.GmallConstant;
import com.microsoft.utils.MyKafkaSender;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @author Jenny.D
 * @create 2021-01-11 6:58
 */
public class CanalClient {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111),
                "example",
                "",
                "");

        while(true){
            canalConnector.connect();
            canalConnector.subscribe("gmall200821.*");
            Message message = canalConnector.get(100);

            if(message.getEntries().size()<=0){
                System.out.println("当前没有数据！休息一会！");
                try{
                    Thread.sleep(5000);
                }catch(InterruptedException e){
                    e.printStackTrace();
                }

            }else{
                List<CanalEntry.Entry> entries = message.getEntries();
                for(CanalEntry.Entry entry:entries){
                    EntryType entryType = entry.getEntryType();

                   if(EntryType.ROWDATA.equals(entryType)){
                       String tableName = entry.getHeader().getTableName();

                       ByteString storeValue = entry.getStoreValue();

                       CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

                       CanalEntry.EventType eventType = rowChange.getEventType();

                       List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                       handler(tableName,eventType,rowDatasList);

                   }

                }

            }


        }

    }

    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        if("order_info".equals(tableName)&& CanalEntry.EventType.INSERT.equals(eventType)){
            sendToKafka(rowDatasList,GmallConstant.GMALL_ORDER_INFO);
        }
    }

    private static void sendToKafka(List<CanalEntry.RowData> rowDatasList, String topic) {

        for(CanalEntry.RowData rowData:rowDatasList){
            JSONObject jsonObject = new JSONObject();
            for(CanalEntry.Column column:rowData.getAfterColumnsList()){
                jsonObject.put(column.getName(), column.getValue());
            }
            System.out.println(jsonObject.toString());
            MyKafkaSender.send(topic,jsonObject.toString());
        }
    }
}
