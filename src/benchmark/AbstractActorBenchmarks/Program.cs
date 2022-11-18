using System.Net;
using Akka.Actor;
using Akka.IO;

namespace AbstractActorBenchmarks
{
    internal class Program
    {
        static void Main(string[] args)
        {
            using var system = ActorSystem.Create("IODocTest");

            var server = system.ActorOf(Server.Props(ActorRefs.Nobody), "server1");

            var count = 100_000;
            for (var i = 0; i < count; i++)
            {
                var client = system.ActorOf(Client.Props(new DnsEndPoint("localhost", 50600), ActorRefs.Nobody));
                client.Tell(new Tcp.Connected(new DnsEndPoint("localhost", 50600), null));
            }

            Console.ReadLine();
            system.Terminate().Wait();
        }
    }

    internal class Server : AbstractActor
    {
        private readonly IActorRef _manager;

        public static Props Props(IActorRef manager) => Akka.Actor.Props.Create<Server>(manager);

        public Server(IActorRef manager) => _manager = manager;

        protected override void PreStart()
        {
            base.PreStart();
            Tcp.Manager(Context.System).Tell(TcpMessage.Bind(Self, new IPEndPoint(IPAddress.Any, 4200), 100), Self);
        }

        protected override Receive CreateReceive =>
            ReceiveBuilder
                .Match<Tcp.Bound>(msg => _manager.Tell((msg, Self)))
                .Match<Tcp.CommandFailed>(_ => Context.Stop(Self))
                .Match<Tcp.Connected>(conn =>
                {
                    _manager.Tell(conn, Self);
                    var handler = Context.ActorOf(Akka.Actor.Props.Create<SimplisticHandler>());
                    Sender.Tell(TcpMessage.Register(handler), Self);
                })
                .Build();
    }

    internal class SimplisticHandler : AbstractActor
    {
        protected override Receive CreateReceive =>
            ReceiveBuilder
                .Match<Tcp.Received>(msg =>
                {
                    var data = msg.Data;
                    Console.Write(data);
                    Sender.Tell(TcpMessage.Write(data), Self);
                })
                .Match<Tcp.ConnectionClosed>(_ => Context.Stop(Self))
                .Build();
    }

    internal class Client : AbstractActor
    {
        private readonly DnsEndPoint _remote;
        private readonly IActorRef _listener;

        public static Props Props(DnsEndPoint remote, IActorRef listener) =>
            Akka.Actor.Props.Create<Client>(remote, listener);

        public Client(DnsEndPoint remote, IActorRef listener)
        {
            _remote = remote;
            _listener = listener;
        }

        protected override void PreStart()
        {
            base.PreStart();
            Tcp.Manager(Context.System).Tell(TcpMessage.Connect(new DnsEndPoint("localhost", 0)), Self);
        }

        protected override Receive CreateReceive =>
            ReceiveBuilder
                .Match<Tcp.CommandFailed>(_ =>
                {
                    _listener.Tell("failed", Self);
                    Context.Stop(Self);
                })
                .Match<Tcp.Connected>(conn =>
                {
                    _listener.Tell(conn, Self);
                    //Sender.Tell(TcpMessage.Register(Self), Self);
                    Context.Become(Connected(Sender));
                })
                .Build();

        protected Receive Connected(IActorRef connection) =>
            ReceiveBuilder
                .Match<ByteString>(msg => connection.Tell(TcpMessage.Write(msg), Self))
                .Match<Tcp.CommandFailed>(_ =>
                {
                    // OS kernel socket buffer was full
                })
                .Match<Tcp.Received>(msg => _listener.Tell(msg.Data, Self))
                //.MatchEquals("close", msg => connection.Tell(TcpMessage.Close(), Self))
                .Match<Tcp.ConnectionClosed>(_ => Context.Stop(Self))
                .Build();
    }
}