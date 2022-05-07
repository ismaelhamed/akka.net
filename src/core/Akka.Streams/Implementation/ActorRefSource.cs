using System;
using Akka.Actor;
using Akka.Annotations;
using Akka.Streams.Stage;
using Akka.Util;
using static Akka.Streams.CompletionStrategy;

namespace Akka.Streams.Implementation
{
    [InternalApi]
    public class ActorRefSource<T> : GraphStageWithMaterializedValue<SourceShape<T>, IActorRef>
    {
        #region Logic

        private sealed class Logic : GraphStageLogic, IActorRefStage
        {
            private readonly ActorRefSource<T> _stage;
            private readonly IMaterializer _eagerMaterializer;
            private readonly Option<IBuffer<T>> _buffer;
            private bool _isCompleting;

            public Logic(ActorRefSource<T> actorRefSource, IMaterializer eagerMaterializer)
                : base(actorRefSource.Shape)
            {
                _stage = actorRefSource;
                _eagerMaterializer = eagerMaterializer;

                _buffer = actorRefSource._maxBuffer != 0
                    ? new Option<IBuffer<T>>(Buffer.Create<T>(actorRefSource._maxBuffer, _eagerMaterializer))
                    : Option<IBuffer<T>>.None; // for backwards compatibility with old actor publisher based implementation

                SetHandler(actorRefSource._out, new LambdaOutHandler(onPull: TryPush));
            }

            public IActorRef Ref => GetEagerStageActor(_eagerMaterializer, poisonPillCompatibility: true, (pair) =>
            {
                var (_, message) = pair;

                switch (message)
                {
                    case PoisonPill _:
                        Log.Warning("for backwards compatibility: PoisonPill will note be supported in the future");
                        CompleteStage();
                        break;
                    case object m when _stage._onFailure != null:
                        FailStage(_stage._onFailure(m));
                        break;
                    case object m when _stage._onCompletion != null:
                        var strategy = _stage._onCompletion(m);
                        if (strategy is Draining)
                        {
                            _isCompleting = true;
                            TryPush();
                        }
                        else if (strategy is Immediately) CompleteStage();
                        break;
                    case T m:
                        {
                            if (!_buffer.HasValue)
                            {
                                if (_isCompleting)
                                    Log.Warning("Dropping element because Status.Success received already: [{0}]", m);
                                else if (IsAvailable(_stage._out))
                                    Push(_stage._out, m);
                                else
                                    Log.Debug("Dropping element because there is no downstream demand and no buffer: [{0}]", m);
                            }
                            else
                            {
                                var buf = _buffer.Value;

                                if (_isCompleting)
                                {
                                    Log.Warning("Dropping element because Status.Success received already, only draining already buffered elements: [{0}] (pending: [{1}])",
                                        m, buf.Used);
                                }
                                else if (!buf.IsFull)
                                {
                                    buf.Enqueue(m);
                                    TryPush();
                                }
                                else
                                {
                                    switch (_stage._overflowStrategy)
                                    {
                                        case OverflowStrategy.DropHead:
                                            {
                                                Log.Debug("Dropping the head element because buffer is full and overflowStrategy is: [DropHead]");
                                                buf.DropHead();
                                                buf.Enqueue(m);
                                                TryPush();
                                            }
                                            break;
                                        case OverflowStrategy.DropTail:
                                            {
                                                Log.Debug("Dropping the tail element because buffer is full and overflowStrategy is: [DropTail]");
                                                buf.DropTail();
                                                buf.Enqueue(m);
                                                TryPush();
                                            }
                                            break;
                                        case OverflowStrategy.DropBuffer:
                                            {
                                                Log.Debug("Dropping all the buffered elements because buffer is full and overflowStrategy is: [DropBuffer]");
                                                buf.Clear();
                                                buf.Enqueue(m);
                                                TryPush();
                                            }
                                            break;
                                        case OverflowStrategy.DropNew:
                                            Log.Debug("Dropping the new element because buffer is full and overflowStrategy is: [DropNew]");
                                            break;
                                        case OverflowStrategy.Fail:
                                            {
                                                Log.Error("Failing because buffer is full and overflowStrategy is: [Fail]");
                                                var bufferOverflowException = new BufferOverflowException($"Buffer overflow (max capacity was: {_stage._maxBuffer})!");
                                                FailStage(bufferOverflowException);
                                            }
                                            break;
                                        case OverflowStrategy.Backpressure:
                                            // there is a precondition check in Source.ActorRefSource factory method to not allow backpressure as strategy
                                            FailStage(new NotSupportedException("Backpressure is not supported"));
                                            break;
                                    }
                                }
                            }
                        }
                        break;
                }
            }).Ref;

            private void TryPush()
            {
                if (IsAvailable(_stage._out) && _buffer != null && !_buffer.Value.IsEmpty)
                {
                    var msg = _buffer.Value.Dequeue();
                    Push(_stage._out, msg);
                }

                if (_isCompleting && (_buffer.IsEmpty || _buffer.Value.IsEmpty))
                {
                    CompleteStage();
                }
            }
        }

        #endregion

        private readonly int _maxBuffer;
        private readonly OverflowStrategy _overflowStrategy;
        private readonly Func<object, ICompletionStrategy> _onCompletion;
        private readonly Func<object, Exception> _onFailure;
        private readonly Outlet<T> _out = new Outlet<T>("actorRefSource.out");

        public ActorRefSource(int maxBuffer, OverflowStrategy overflowStrategy, Func<object, ICompletionStrategy> onCompletion, Func<object, Exception> onFailure)
        {
            _maxBuffer = maxBuffer;
            _overflowStrategy = overflowStrategy;
            _onCompletion = onCompletion;
            _onFailure = onFailure;

            Shape = new SourceShape<T>(_out);
        }

        public override SourceShape<T> Shape { get; }

        public override ILogicAndMaterializedValue<IActorRef> CreateLogicAndMaterializedValue(Attributes inheritedAttributes) =>
            throw new InvalidOperationException("Not supported");

        internal override ILogicAndMaterializedValue<IActorRef> CreateLogicAndMaterializedValue(Attributes inheritedAttributes, IMaterializer eagerMaterializer)
        {
            var logic = new Logic(this, eagerMaterializer);
            return new LogicAndMaterializedValue<IActorRef>(logic, logic.Ref);
        }
    }

    internal interface IActorRefStage
    {
        IActorRef Ref { get; }
    }
}
