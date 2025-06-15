using System.IO.Pipes;


Console.Title = "Console Pipe OUT";

Console.OutputEncoding = System.Text.Encoding.UTF8;
Task connectTask = Task.Run(() => ConnectToServerAndProcess());
await Task.Delay(2000); // Đợi 2 giây giữa các lần in số

for (int i = 0; i < 10; i++)
{
    Console.WriteLine(i.ToString());
    await Task.Delay(2000); // Đợi 2 giây giữa các lần in số
}

//Console.ReadLine();
await connectTask;
//using (var pipeClient = new NamedPipeClientStream(".", "testpipe", PipeDirection.Out))
//{
//    Console.WriteLine("Connecting to server...");
//    pipeClient.Connect();

//    Console.WriteLine("Connected.");

//    Task.Run(async () => await ReadConsoleAndSendToPipe(pipeClient));


//    for (int i = 0; i < 10; i++)
//    {
//        Console.WriteLine(i.ToString());
//        Task.Delay(2000);
//    }

//    Console.ReadLine();
//}

static async Task ReadConsoleAndSendToPipe(NamedPipeClientStream pipeClient)
{
    using (var writer = new StreamWriter(pipeClient))
    {
        writer.AutoFlush = true;
        // Chuyển hướng Console output tới NamedPipe
        Console.SetOut(writer);


        string message;
        while ((message = Console.ReadLine()) != null)
        {
            Console.WriteLine(message);
        }
    }
}

static async Task ConnectToServerAndProcess()
{
    while (true)
    {
        try
        {
            using (var pipeClient = new NamedPipeClientStream(".", "testpipe", PipeDirection.Out))
            {
                Console.WriteLine("Attempting to connect to server...");

                // Thử kết nối lại sau mỗi 1 giây nếu thất bại
                while (!pipeClient.IsConnected)
                {
                    try
                    {
                        pipeClient.Connect(1000); // Thời gian chờ 1 giây
                    }
                    catch (TimeoutException)
                    {
                        Console.WriteLine("Server not available. Retrying in 1 second...");
                        await Task.Delay(1000);
                    }
                }

                Console.WriteLine("Connected to server.");

                // Đợi 1 giây sau khi kết nối
                await Task.Delay(1000);

                // Chạy đọc console và gửi dữ liệu tới pipe trên một luồng riêng
                await ReadConsoleAndSendToPipe(pipeClient);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"An error occurred: {ex.Message}");
        }
    }
}


//using (NamedPipeServerStream pipeServer =
//            new NamedPipeServerStream("testpipe", PipeDirection.Out))
//{
//    Console.WriteLine("NamedPipeServerStream object created.");

//    // Wait for a client to connect
//    Console.Write("Waiting for client connection...");
//    pipeServer.WaitForConnection();

//    Console.WriteLine("Client connected.");
//    try
//    {
//        // Read user input and send that to the client process.
//        using (StreamWriter sw = new StreamWriter(pipeServer))
//        {
//            sw.AutoFlush = true;
//            Console.Write("Enter text: ");
//            sw.WriteLine(Console.ReadLine());
//        }
//    }
//    // Catch the IOException that is raised if the pipe is broken
//    // or disconnected.
//    catch (IOException e)
//    {
//        Console.WriteLine("ERROR: {0}", e.Message);
//    }
//}

//using (NamedPipeServerStream pipeServer =
//            new NamedPipeServerStream("testpipe", PipeDirection.Out))
//{
//    Console.WriteLine("NamedPipeServerStream object created.");

//    // Wait for a client to connect
//    Console.Write("Waiting for client connection...");
//    pipeServer.WaitForConnection();

//    Console.WriteLine("Client connected.");

//    Task.Run(() => ReadConsoleAndSendToPipe(pipeServer));

//    // Ghi dữ liệu vào Console.Out
//    Console.WriteLine("Hello from Console!");
//    Console.WriteLine("This is another line.");
//    Console.ReadLine();

//try
//{
//    // Read user input and send that to the client process.
//    using (StreamWriter sw = new StreamWriter(pipeServer))
//    {
//        sw.AutoFlush = true;
//        Console.WriteLine("ABC");
//        Console.Write("Enter text: ");
//        sw.WriteLine(Console.ReadLine());
//    }
//}
//// Catch the IOException that is raised if the pipe is broken
//// or disconnected.
//catch (IOException e)
//{
//    Console.WriteLine("ERROR: {0}", e.Message);
//}
////}