package com.ibeifeng.sparkproject.test.streaming.socket;


import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
public class StreamSocketServer {
	 public static void main(String[] args) {
         try {
             ServerSocket serverSocket = new ServerSocket(9999);
             Socket socket=null;
             while(true) {
                 socket = serverSocket.accept();
                 while(socket.isConnected()) {
                     // 向服务器端发送数据
                     OutputStream os =  socket.getOutputStream();
                     DataOutputStream bos = new DataOutputStream(os);
                     //每隔20ms发送一次数据
                     String str="Connect 123 test spark streaming abc xyz hik\n";
                     while(true){
                         bos.writeUTF(new Date()+" ----- "+str);
                         bos.flush();
                         try {
                             Thread.sleep(200);
                         } catch (InterruptedException e) {
                             e.printStackTrace();
                         }
                     }
                 }
                 System.out.println("------------");
             }
         } catch (IOException e) {
             e.printStackTrace();
         }
     }

}
