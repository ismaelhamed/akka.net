//-----------------------------------------------------------------------
// <copyright file="PartialActionBuilderTests.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2022 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Reflection;
using System.Runtime.CompilerServices;
using Akka.Actor;
using Akka.TestKit;
using Akka.Tools.MatchHandler;
using Xunit;

namespace Akka.Tests.MatchHandler
{
    public static class ReceiveExtensions
    {
        public static Receive AndThen(this Receive target, Receive then)
        {
            bool t(object o)
            {
                var r = target?.Invoke(o);
                return r.HasValue && r.Value && (then?.Invoke(o) ?? false);
            }
            return t;
        }

        public static Receive OrElse(this Receive target, Receive that)
        {
            bool t(object o) => target?.Invoke(o) ?? that?.Invoke(o) ?? false;
            return t;
        }
    }

    public class ReceiveDelegateTests : AkkaSpec
    {
        [Fact]
        public void Receive_test_1()
        {
            Receive rec1 = default;
            Receive rec2 = default;
            Assert.False(rec1.AndThen(rec2)(12));
        }

        [Fact]
        public void Receive_test_2()
        {
            Receive rec1 = _ => true;
            Receive rec2 = o =>
            {
                if ((int)o == 12) return true;
                return false;
            };
            Assert.True(rec1.AndThen(rec2)(12));
        }

        [Fact]
        public void Receive_test_3()
        {
            Receive rec1 = default;
            Receive rec2 = o =>
            {
                if ((int)o == 12) return true;
                return false;
            };
            Assert.True(rec1.OrElse(rec2)(12));
        }

        [Fact]
        public void Receive_test_4()
        {
            Receive rec1 = _ => true;
            Receive rec2 = o => false;
            var rec3 = Delegate.Combine(rec1, rec2);

            var multicastList = (rec3 as MulticastDelegate)?.GetInvocationList();
            if (multicastList != null)
            {

            }

            //Assert.True(rec3(12));


            PartialF p1 = default;
            PartialF p2 = (apply, isDefinedAt) =>
            {
                if (isDefinedAt(apply))
                    return true;
                return false;
            };

            var p3 = p1.OrElse(p2);
            var r = p3(12);
        }
    }

    // https://softwareengineering.stackexchange.com/questions/345039/why-isnt-it-common-to-hack-partial-function-application-in-c

    public delegate bool PartialF(object apply, Predicate<object> isDefinedAt = default);

    public static class PartialFExtensions
    {
        //var negativeOrZeroToPositive = PartialFuncion<int>.Create(x => Math.Abs(x), x => x <= 0);

        public static PartialF OrElse(this PartialF target, PartialF that)
        {
            bool t(object o, Predicate<object> isDefinedAt)
            {
                return isDefinedAt != null && isDefinedAt(o)
                    ? target.Invoke(o)
                    : that.Invoke(o);
            }
            return t;
        }
    }

    // https://www.baeldung.com/scala/partial-functions
    public readonly struct PartialFuncion<T>
    {        
        private readonly Func<T, T> _apply;
        private readonly Predicate<T> _isDefinedAt;

        public static PartialFuncion<T> Create(Func<T, T> apply) =>
            new PartialFuncion<T>(apply);

        public static PartialFuncion<T> Create(Func<T, T> apply, Predicate<T> isDefinedAt) =>
           new PartialFuncion<T>(apply, isDefinedAt);

        public PartialFuncion(Func<T, T> apply, Predicate<T> isDefinedAt = default)
        {
            _apply = apply;
            _isDefinedAt = isDefinedAt ?? (_ => true);
        }

        //public PartialFuncion<T> OrElse(PartialFuncion<T> that)
        //{
        //    bool t(T o) => IsDefinedAt != null && IsDefinedAt(o);
        //    return IsDefinedAt == null ? that : this;
        //}

        //public Delegate CreateDelegate(Delegate previous)
        //{
        //    // https://particular.net/blog/10x-faster-execution-with-compiled-expression-trees
        //}
    }

    //public static class PartialFuncion
    //{
    //    public static PartialFuncion<object> OrElse(this PartialFuncion<object> target, PartialFuncion<object> that)
    //    {
    //        bool t(object o) => target.IsDefinedAt != null && target.IsDefinedAt(o)
    //            ? target.Apply(o)
    //            : that;

    //        return t;
    //    }
    //}

    public class PartialActionBuilderTests : AkkaSpec
    {
        [Fact]
        public void Given_a_0_arguments_delegate_When_building_and_invoking_Then_the_supplied_function_is_called()
        {
            var builder = new PartialActionBuilder();
            object updatedValue = null;
            Func<object, bool> deleg = value => { updatedValue = value; return true; };

            var partialAction = builder.Build<object>(new CompiledMatchHandlerWithArguments(deleg, new object[0]));
            partialAction("value");
            Assert.Same(updatedValue, "value");
        }

        [Fact]
        public void Given_a_1_arguments_delegate_When_building_and_invoking_Then_the_supplied_function_is_called()
        {
            var builder = new PartialActionBuilder();
            object updatedValue = null;
            var delegateArguments = new object[] { "a1" };
            var updatedArgs = new object[delegateArguments.Length];
            Func<object, string, bool> deleg = (value, a1) =>
            {
                updatedValue = value;
                updatedArgs[0] = a1;
                return true;
            };

            var partialAction = builder.Build<object>(new CompiledMatchHandlerWithArguments(deleg, delegateArguments));
            partialAction("value");
            Assert.Same("value", updatedValue);
            AssertAreSame(delegateArguments, updatedArgs);
        }

        [Fact]
        public void Given_a_2_arguments_delegate_When_building_and_invoking_Then_the_supplied_function_is_called()
        {
            var builder = new PartialActionBuilder();
            object updatedValue = null;
            var delegateArguments = new object[] { "a1", 1 };
            var updatedArgs = new object[delegateArguments.Length];
            Func<object, string, int, bool> deleg = (value, a1, a2) =>
            {
                updatedValue = value;
                updatedArgs[0] = a1;
                updatedArgs[1] = a2;
                return true;
            };

            var partialAction = builder.Build<object>(new CompiledMatchHandlerWithArguments(deleg, delegateArguments));
            partialAction("value");
            Assert.Same("value", updatedValue);
            AssertAreSame(delegateArguments, updatedArgs);
        }


        [Fact]
        public void Given_a_3_arguments_delegate_When_building_and_invoking_Then_the_supplied_function_is_called()
        {
            var builder = new PartialActionBuilder();
            object updatedValue = null;
            var delegateArguments = new object[] { "a1", 1, 3.0f };
            var updatedArgs = new object[delegateArguments.Length];
            Func<object, string, int, float, bool> deleg = (value, a1, a2, a3) =>
            {
                updatedValue = value;
                updatedArgs[0] = a1;
                updatedArgs[1] = a2;
                updatedArgs[2] = a3;
                return true;
            };

            var partialAction = builder.Build<object>(new CompiledMatchHandlerWithArguments(deleg, delegateArguments));
            partialAction("value");
            Assert.Same("value", updatedValue);
            AssertAreSame(delegateArguments, updatedArgs);
        }

        [Fact]
        public void Given_a_4_arguments_delegate_When_building_and_invoking_Then_the_supplied_function_is_called()
        {
            var builder = new PartialActionBuilder();
            object updatedValue = null;
            var delegateArguments = new object[] { "a1", 1, 3.0f, "a4" };
            var updatedArgs = new object[delegateArguments.Length];
            Func<object, string, int, float, string, bool> deleg = (value, a1, a2, a3, a4) =>
            {
                updatedValue = value;
                updatedArgs[0] = a1;
                updatedArgs[1] = a2;
                updatedArgs[2] = a3;
                updatedArgs[3] = a4;
                return true;
            };

            var partialAction = builder.Build<object>(new CompiledMatchHandlerWithArguments(deleg, delegateArguments));
            partialAction("value");
            Assert.Same("value", updatedValue);
            AssertAreSame(delegateArguments, updatedArgs);
        }

        [Fact]
        public void Given_a_5_arguments_delegate_When_building_and_invoking_Then_the_supplied_function_is_called()
        {
            var builder = new PartialActionBuilder();
            object updatedValue = null;
            var delegateArguments = new object[] { "a1", 1, 3.0f, "a4", 5 };
            var updatedArgs = new object[delegateArguments.Length];
            Func<object, string, int, float, string, int, bool> deleg = (value, a1, a2, a3, a4, a5) =>
            {
                updatedValue = value;
                updatedArgs[0] = a1;
                updatedArgs[1] = a2;
                updatedArgs[2] = a3;
                updatedArgs[3] = a4;
                updatedArgs[4] = a5;
                return true;
            };

            var partialAction = builder.Build<object>(new CompiledMatchHandlerWithArguments(deleg, delegateArguments));
            partialAction("value");
            Assert.Same("value", updatedValue);
            AssertAreSame(delegateArguments, updatedArgs);
        }

        [Fact]
        public void Given_a_6_arguments_delegate_When_building_and_invoking_Then_the_supplied_function_is_called()
        {
            var builder = new PartialActionBuilder();
            object updatedValue = null;
            var delegateArguments = new object[] { "a1", 1, 3.0f, "a4", 5, 6.0f };
            var updatedArgs = new object[delegateArguments.Length];
            Func<object, string, int, float, string, int, float, bool> deleg = (value, a1, a2, a3, a4, a5, a6) =>
            {
                updatedValue = value;
                updatedArgs[0] = a1;
                updatedArgs[1] = a2;
                updatedArgs[2] = a3;
                updatedArgs[3] = a4;
                updatedArgs[4] = a5;
                updatedArgs[5] = a6;
                return true;
            };

            var partialAction = builder.Build<object>(new CompiledMatchHandlerWithArguments(deleg, delegateArguments));
            partialAction("value");
            Assert.Same("value", updatedValue);
            AssertAreSame(delegateArguments, updatedArgs);
        }

        [Fact]
        public void Given_a_7_arguments_delegate_When_building_and_invoking_Then_the_supplied_function_is_called()
        {
            var builder = new PartialActionBuilder();
            object updatedValue = null;
            var delegateArguments = new object[] { "a1", 1, 3.0f, "a4", 5, 6.0f, "a7" };
            var updatedArgs = new object[delegateArguments.Length];
            Func<object, string, int, float, string, int, float, string, bool> deleg = (value, a1, a2, a3, a4, a5, a6, a7) =>
            {
                updatedValue = value;
                updatedArgs[0] = a1;
                updatedArgs[1] = a2;
                updatedArgs[2] = a3;
                updatedArgs[3] = a4;
                updatedArgs[4] = a5;
                updatedArgs[5] = a6;
                updatedArgs[6] = a7;
                return true;
            };

            var partialAction = builder.Build<object>(new CompiledMatchHandlerWithArguments(deleg, delegateArguments));
            partialAction("value");
            Assert.Same("value", updatedValue);
            AssertAreSame(delegateArguments, updatedArgs);
        }

        [Fact]
        public void Given_a_8_arguments_delegate_When_building_and_invoking_Then_the_supplied_function_is_called()
        {
            var builder = new PartialActionBuilder();
            object updatedValue = null;
            var delegateArguments = new object[] { "a1", 1, 3.0f, "a4", 5, 6.0f, "a7", 8 };
            var updatedArgs = new object[delegateArguments.Length];
            Func<object, string, int, float, string, int, float, string, int, bool> deleg = (value, a1, a2, a3, a4, a5, a6, a7, a8) =>
            {
                updatedValue = value;
                updatedArgs[0] = a1;
                updatedArgs[1] = a2;
                updatedArgs[2] = a3;
                updatedArgs[3] = a4;
                updatedArgs[4] = a5;
                updatedArgs[5] = a6;
                updatedArgs[6] = a7;
                updatedArgs[7] = a8;
                return true;
            };

            var partialAction = builder.Build<object>(new CompiledMatchHandlerWithArguments(deleg, delegateArguments));
            partialAction("value");
            Assert.Same("value", updatedValue);
            AssertAreSame(delegateArguments, updatedArgs);
        }

        [Fact]
        public void Given_a_9_arguments_delegate_When_building_and_invoking_Then_the_supplied_function_is_called()
        {
            var builder = new PartialActionBuilder();
            object updatedValue = null;
            var delegateArguments = new object[] { "a1", 1, 3.0f, "a4", 5, 6.0f, "a7", 8, 9.0f };
            var updatedArgs = new object[delegateArguments.Length];
            Func<object, string, int, float, string, int, float, string, int, float, bool> deleg = (value, a1, a2, a3, a4, a5, a6, a7, a8, a9) =>
            {
                updatedValue = value;
                updatedArgs[0] = a1;
                updatedArgs[1] = a2;
                updatedArgs[2] = a3;
                updatedArgs[3] = a4;
                updatedArgs[4] = a5;
                updatedArgs[5] = a6;
                updatedArgs[6] = a7;
                updatedArgs[7] = a8;
                updatedArgs[8] = a9;
                return true;
            };

            var partialAction = builder.Build<object>(new CompiledMatchHandlerWithArguments(deleg, delegateArguments));
            partialAction("value");
            Assert.Same("value", updatedValue);
            AssertAreSame(delegateArguments, updatedArgs);
        }


        [Fact]
        public void Given_a_10_arguments_delegate_When_building_and_invoking_Then_the_supplied_function_is_called()
        {
            var builder = new PartialActionBuilder();
            object updatedValue = null;
            var delegateArguments = new object[] { "a1", 1, 3.0f, "a4", 5, 6.0f, "a7", 8, 9.0f, "a10" };
            var updatedArgs = new object[delegateArguments.Length];
            Func<object, string, int, float, string, int, float, string, int, float, string, bool> deleg = (value, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10) =>
            {
                updatedValue = value;
                updatedArgs[0] = a1;
                updatedArgs[1] = a2;
                updatedArgs[2] = a3;
                updatedArgs[3] = a4;
                updatedArgs[4] = a5;
                updatedArgs[5] = a6;
                updatedArgs[6] = a7;
                updatedArgs[7] = a8;
                updatedArgs[8] = a9;
                updatedArgs[9] = a10;
                return true;
            };

            var partialAction = builder.Build<object>(new CompiledMatchHandlerWithArguments(deleg, delegateArguments));
            partialAction("value");
            Assert.Same("value", updatedValue);
            AssertAreSame(delegateArguments, updatedArgs);
        }

        [Fact]
        public void Given_a_11_arguments_delegate_When_building_and_invoking_Then_the_supplied_function_is_called()
        {
            var builder = new PartialActionBuilder();
            object updatedValue = null;
            var delegateArguments = new object[] { "a1", 1, 3.0f, "a4", 5, 6.0f, "a7", 8, 9.0f, "a10", 11 };
            var updatedArgs = new object[delegateArguments.Length];
            Func<object, string, int, float, string, int, float, string, int, float, string, int, bool> deleg = (value, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11) =>
            {
                updatedValue = value;
                updatedArgs[0] = a1;
                updatedArgs[1] = a2;
                updatedArgs[2] = a3;
                updatedArgs[3] = a4;
                updatedArgs[4] = a5;
                updatedArgs[5] = a6;
                updatedArgs[6] = a7;
                updatedArgs[7] = a8;
                updatedArgs[8] = a9;
                updatedArgs[9] = a10;
                updatedArgs[10] = a11;
                return true;
            };

            var partialAction = builder.Build<object>(new CompiledMatchHandlerWithArguments(deleg, delegateArguments));
            partialAction("value");
            Assert.Same("value", updatedValue);
            AssertAreSame(delegateArguments, updatedArgs);
        }

        [Fact]
        public void Given_a_12_arguments_delegate_When_building_and_invoking_Then_the_supplied_function_is_called()
        {
            var builder = new PartialActionBuilder();
            object updatedValue = null;
            var delegateArguments = new object[] { "a1", 1, 3.0f, "a4", 5, 6.0f, "a7", 8, 9.0f, "a10", 11, 12f };
            var updatedArgs = new object[delegateArguments.Length];
            Func<object, string, int, float, string, int, float, string, int, float, string, int, float, bool> deleg = (value, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12) =>
            {
                updatedValue = value;
                updatedArgs[0] = a1;
                updatedArgs[1] = a2;
                updatedArgs[2] = a3;
                updatedArgs[3] = a4;
                updatedArgs[4] = a5;
                updatedArgs[5] = a6;
                updatedArgs[6] = a7;
                updatedArgs[7] = a8;
                updatedArgs[8] = a9;
                updatedArgs[9] = a10;
                updatedArgs[10] = a11;
                updatedArgs[11] = a12;
                return true;
            };

            var partialAction = builder.Build<object>(new CompiledMatchHandlerWithArguments(deleg, delegateArguments));
            partialAction("value");
            Assert.Same("value", updatedValue);
            AssertAreSame(delegateArguments, updatedArgs);
        }

        [Fact]
        public void Given_a_13_arguments_delegate_When_building_and_invoking_Then_the_supplied_function_is_called()
        {
            var builder = new PartialActionBuilder();
            object updatedValue = null;
            var delegateArguments = new object[] { "a1", 1, 3.0f, "a4", 5, 6.0f, "a7", 8, 9.0f, "a10", 11, 12f, "a13" };
            var updatedArgs = new object[delegateArguments.Length];
            Func<object, string, int, float, string, int, float, string, int, float, string, int, float, string, bool> deleg = (value, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13) =>
            {
                updatedValue = value;
                updatedArgs[0] = a1;
                updatedArgs[1] = a2;
                updatedArgs[2] = a3;
                updatedArgs[3] = a4;
                updatedArgs[4] = a5;
                updatedArgs[5] = a6;
                updatedArgs[6] = a7;
                updatedArgs[7] = a8;
                updatedArgs[8] = a9;
                updatedArgs[9] = a10;
                updatedArgs[10] = a11;
                updatedArgs[11] = a12;
                updatedArgs[12] = a13;
                return true;
            };

            var partialAction = builder.Build<object>(new CompiledMatchHandlerWithArguments(deleg, delegateArguments));
            partialAction("value");
            Assert.Same("value", updatedValue);
            AssertAreSame(delegateArguments, updatedArgs);
        }

        [Fact]
        public void Given_a_14_arguments_delegate_When_building_and_invoking_Then_the_supplied_function_is_called()
        {
            var builder = new PartialActionBuilder();
            object updatedValue = null;
            var delegateArguments = new object[] { "a1", 1, 3.0f, "a4", 5, 6.0f, "a7", 8, 9.0f, "a10", 11, 12f, "a13", 14 };
            var updatedArgs = new object[delegateArguments.Length];
            Func<object, string, int, float, string, int, float, string, int, float, string, int, float, string, int, bool> deleg = (value, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14) =>
            {
                updatedValue = value;
                updatedArgs[0] = a1;
                updatedArgs[1] = a2;
                updatedArgs[2] = a3;
                updatedArgs[3] = a4;
                updatedArgs[4] = a5;
                updatedArgs[5] = a6;
                updatedArgs[6] = a7;
                updatedArgs[7] = a8;
                updatedArgs[8] = a9;
                updatedArgs[9] = a10;
                updatedArgs[10] = a11;
                updatedArgs[11] = a12;
                updatedArgs[12] = a13;
                updatedArgs[13] = a14;
                return true;
            };

            var partialAction = builder.Build<object>(new CompiledMatchHandlerWithArguments(deleg, delegateArguments));
            partialAction("value");
            Assert.Same("value", updatedValue);
            AssertAreSame(delegateArguments, updatedArgs);
        }


        [Fact]
        public void Given_a_15_arguments_delegate_When_building_and_invoking_Then_the_supplied_function_is_called()
        {
            var builder = new PartialActionBuilder();
            object updatedValue = null;
            var delegateArguments = new object[] { "a1", 1, 3.0f, "a4", 5, 6.0f, "a7", 8, 9.0f, "a10", 11, 12f, "a13", 14, 15f };
            var updatedArgs = new object[delegateArguments.Length];
            Func<object, string, int, float, string, int, float, string, int, float, string, int, float, string, int, float, bool> deleg = (value, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15) =>
            {
                updatedValue = value;
                updatedArgs[0] = a1;
                updatedArgs[1] = a2;
                updatedArgs[2] = a3;
                updatedArgs[3] = a4;
                updatedArgs[4] = a5;
                updatedArgs[5] = a6;
                updatedArgs[6] = a7;
                updatedArgs[7] = a8;
                updatedArgs[8] = a9;
                updatedArgs[9] = a10;
                updatedArgs[10] = a11;
                updatedArgs[11] = a12;
                updatedArgs[12] = a13;
                updatedArgs[13] = a14;
                updatedArgs[14] = a15;
                return true;
            };

            var partialAction = builder.Build<object>(new CompiledMatchHandlerWithArguments(deleg, delegateArguments));
            partialAction("value");
            Assert.Same("value", updatedValue);
            AssertAreSame(delegateArguments, updatedArgs);
        }


        [Fact]
        public void When_building_with_16_args_Then_it_fails()
        {
            var builder = new PartialActionBuilder();
            var delegateArguments = new object[] { "a1", 1, 3.0f, "a4", 5, 6.0f, "a7", 8, 9.0f, "a10", 11, 12f, "a13", 14, 15f, "a16" };
            Assert.Throws<ArgumentException>(() => ((Action)(() => builder.Build<object>(new CompiledMatchHandlerWithArguments(null, delegateArguments))))());
        }

        private static void AssertAreSame(object[] delegateArguments, object[] updatedArgs)
        {
            for (int i = 0; i < delegateArguments.Length; i++)
            {
                if (delegateArguments[i].GetType().GetTypeInfo().IsValueType)
                    Assert.Equal(delegateArguments[i], updatedArgs[i]);
                else
                    Assert.Same(delegateArguments[i], updatedArgs[i]);
            }
        }
    }
}

