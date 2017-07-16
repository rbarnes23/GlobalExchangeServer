package com.mapmymotion.globalexchangeserver;
/**
 * Nakov Chat Server
 * (c) Svetlin Nakov, 2002
 * http://www.nakov.com
 *
 * Nakov Chat Server is multithreaded chat server. It accepts multiple clients
 * simultaneously and serves them. Clients can send messages to the server.
 * When some client send a message to the server, this message is dispatched
 * to all the clients connected to the server.
 *
 * The server consists of two components - "server core" and "client handlers".
 *
 * The "server core" consists of two threads:
 *   - GlobalExchangeServer - accepts client connections, creates client threads to
 * handle them and starts these threads
 *   - ServerDispatcher - waits for a messages and sends arrived messages to
 * all the clients connected to the server
 *
 * The "client handlers" consist of two threads:
 *   - ClientListener - listens for message arrivals from the socket and
 * forwards them to the ServerDispatcher thread
 *   - ClientSender - sends messages to the client
 *
 * For each accepted client, a ClientListener and ClientSender threads are
 * created and started. A ClientInfo object is also created to contain the
 * information about the client. Also the ClientInfo object is added to the
 * ServerDispatcher's clients list. When some client is disconnected, is it
 * removed from the clients list and both its ClientListener and ClientSender
 * threads are interrupted.
 *
 *
 * GlobalExchangeServer class is entry point for the program. It opens a server
 * socket, starts the dispatcher thread and infinitely accepts client connections,
 * creates threads for handling them and starts these threads.
 */
 
import java.net.*;
import java.io.*;
 
public class GlobalExchangeServer
{
	// When switching the port for either MapMyMotion or ArtytheArtist go to SQL and change database url and dbname
    //public static final int LISTENING_PORT = 2002;
	public static final int LISTENING_PORT = 2003;
	//public static final int LISTENING_PORT = 2004;
 
    public static void main(String[] args)
    {

        // Open server socket for listening
        ServerSocket serverSocket = null;
        try {
           serverSocket = new ServerSocket(LISTENING_PORT);
           System.out.println("GlobalExchangeServer started on port " + LISTENING_PORT);
        } catch (IOException se) {
           System.err.println("Can not start listening on port " + LISTENING_PORT);
           se.printStackTrace();
           System.exit(-1);
        }
 
        // Start ServerDispatcher thread
        ServerDispatcher serverDispatcher = new ServerDispatcher();
        serverDispatcher.start();
 
		

        // Accept and handle client connections
        while (true) {
           try {
               Socket socket = serverSocket.accept();
               ClientInfo clientInfo = new ClientInfo();  //Generate ObjectId in HERE to use as SessionID
        
               clientInfo.mSocket = socket;
               ClientListener clientListener =
                   new ClientListener(clientInfo, serverDispatcher);
               ClientSender clientSender =
                   new ClientSender(clientInfo, serverDispatcher);
               clientInfo.mClientListener = clientListener;
               clientInfo.mClientSender = clientSender;
               serverDispatcher.addClient(clientInfo);
               clientListener.start();
               clientSender.start();
               //Not add client here - only at login
               //serverDispatcher.addClient(clientInfo);
           } catch (IOException ioe) {
               ioe.printStackTrace();
           }
        }
    }
 
}
 