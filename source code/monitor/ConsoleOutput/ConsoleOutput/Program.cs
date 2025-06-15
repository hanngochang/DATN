using System.IO.Pipes;

Console.Title = "Console Pipe IN";
Console.Title = "APP.API Monitor";

Console.OutputEncoding = System.Text.Encoding.UTF8;
//using (var pipeServer = new NamedPipeServerStream("testpipe", PipeDirection.In))
//{
//    Console.WriteLine("Waiting for connection...");
//    pipeServer.WaitForConnection();

//    using (var reader = new StreamReader(pipeServer))
//    {
//        string message;
//        while ((message = reader.ReadLine()) != null)
//        {
//            Console.WriteLine("Received: " + message);
//        }
//    }
//}

Console.WriteLine("Starting server...");

while (true)
{
    using (var pipeServer = new NamedPipeServerStream("testpipe", PipeDirection.In, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous))
    {
        Console.WriteLine("Waiting for connection...");
        await pipeServer.WaitForConnectionAsync();

        Console.WriteLine("Client connected.");
        await Task.Run(() => HandleClient(pipeServer));
    }
}
static void HandleClient(NamedPipeServerStream pipeServer)
{
    using (var reader = new StreamReader(pipeServer))
    {
        string message;
        try
        {
            while ((message = reader.ReadLine()) != null)
            {
                Console.WriteLine("Received: " + message);
            }
        }
        catch (IOException)
        {
            // Client disconnected
            Console.WriteLine("Client disconnected.");
        }
    }
}