package com.mapmymotion.globalexchangeserver;
/**
 * Nakov Chat Server
 * (c) Svetlin Nakov, 2002
 *
 * ClientInfo class contains information about a client, connected to the server.
 */
 
import java.net.Socket;
import org.bson.types.ObjectId;
 
public class ClientInfo
{
    public Socket mSocket = null;
    public ClientListener mClientListener = null;
    public ClientSender mClientSender = null;
    public static ObjectId sSessionId = new ObjectId();  // Use to tie sessions together
}