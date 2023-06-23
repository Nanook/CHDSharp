using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Linq;

namespace CHDSharpLib.Utils;

//2023-06-23 - Nanook

internal class BlockingThreadQueue<T>
{
    private class threadState
    {
        public int Index;
        public object Object;
        public bool Allocated;
    }

    private object _lq;
    private object _lp;
    private Queue<T> _q;
    private volatile int _tc;
    private bool _complete;

    private threadState[] _threads; //logical thread indexes for the caller to resue resource (e.g. an array of buffers)

    public BlockingThreadQueue(int maxQueue, int maxParallel)
    {
        _lq = new object();
        _lp = new object();

        _q = new Queue<T>();

        this.MaxQueue = maxQueue;
        this.MaxParallel = maxParallel;
        _threads = new threadState[maxParallel];
        for (int i = 0; i < this.MaxParallel; i++)
            _threads[i] = new threadState() { Index = i, Object = null };
    }

    public int MaxQueue { get; }
    public int MaxParallel { get; }

    public int QueueCount => _q.Count;

    /// <summary>
    /// Single thread Add. Locks when maxQueue is reached.
    /// </summary>
    /// <param name="item"></param>
    public void Add(T item)
    {
        lock (_lq)
        {
            if (_complete)
                throw new Exception("Items can not be added when Complete");

            if (_q.Count == this.MaxQueue) //never add more
                Monitor.Wait(_lq);
            _q.Enqueue(item);
            Monitor.Pulse(_lq);
        }
    }

    public void AddComplete()
    {
        lock (_lq)
        {
            _complete = true;
            Monitor.Pulse(_lq); //pause
        }
    }

    public Task Process(Action<T, int> process)
    {
        return new TaskFactory().StartNew(() =>
        {
            int tc = this.MaxParallel;
            bool exit = false;

            while (!exit)
            {
                threadState state;
                lock (_lq)
                {
                    while (!exit && _q.Count == 0)
                    {
                        if (_complete)
                            exit = true;
                        else
                            Monitor.Wait(_lq); //pause
                    }
                    if (exit)
                        break;

                    lock (_lp)
                    {
                        if (_tc == tc)
                            Monitor.Wait(_lp); //wait for a thread to complete and free up a slot
                        _tc++; //has item and thread - lets go
                        state = _threads.First(a => !a.Allocated);
                        state.Allocated = true;
                    }
                    state.Object = _q.Dequeue(); //dequeue item and allocate
                    Monitor.Pulse(_lq); //allow another item to be queued
                }

                ThreadPool.QueueUserWorkItem(obj =>
                {
                    try
                    {
                        threadState ts = (threadState)obj;
                        process((T)ts.Object, (int)ts.Index);
                        //Debug.WriteLine($"{obj}");
                    }
                    finally
                    {
                        lock (_lp)
                        {
                            state.Object = null; //release
                            state.Allocated = false;
                            _tc--; //complete
                            Monitor.Pulse(_lp); //allow another thread to go
                            //Debug.WriteLine($"ThreadCount: {_tc}");
                        }
                    }
                }, state);
            }

            while (true)
            {
                lock (_lp)
                {
                    if (_tc == 0)
                        return;
                    Monitor.Wait(_lp, 10);
                    Monitor.Pulse(_lp);
                }
            }
        });
    }
}
