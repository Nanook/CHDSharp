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
        public int ThreadIndex;
        public object Object;
        public bool Allocated;
        public long Index;
        public override string ToString()
        {
            return $"{Index}";
        }
    }

    private class finaliseState
    {
        public threadState ThreadState;
        public long FinaliseIndex;
        public override string ToString()
        {
            return $"{FinaliseIndex} : {ThreadState?.Index}";
        }
    }

    private object _lq;
    private object _lp;
    private object _lf;
    private Queue<T> _q;
    private volatile int _tc;
    private long _fc;
    private long _fp; //finalise process
    private bool _complete;

    private threadState[] _threadsState; //logical thread indexes for the caller to resue resource (e.g. an array of buffers)
    private finaliseState[] _finaliseState; //logical thread indexes for the caller to resue resource (e.g. an array of buffers)

    public BlockingThreadQueue(int maxQueue, int maxParallel, bool useFinalise)
    {
        _lq = new object();
        _lp = new object();
        _lf = new object();
        _q = new Queue<T>();

        this.MaxQueue = maxQueue;
        this.MaxParallel = maxParallel;
        UseFinalise = useFinalise;
        _threadsState = new threadState[maxParallel];
        for (int i = 0; i < _threadsState.Length; i++)
            _threadsState[i] = new threadState() { ThreadIndex = i, Object = null };

        _finaliseState = new finaliseState[maxParallel];
        for (int i = 0; i < _finaliseState.Length; i++)
            _finaliseState[i] = new finaliseState() { FinaliseIndex = i, ThreadState = null };
    }

    public int MaxQueue { get; }
    public int MaxParallel { get; }
    public bool UseFinalise { get; }
    public int QueueCount => _q.Count;
    public int ProcessingCount => _tc;


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

            while (_q.Count == this.MaxQueue) //never add more
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
            Monitor.Pulse(_lq);
        }
    }

    public Task Process(Action<T, int> process, Action<T> finalise)
    {
        if (this.UseFinalise && finalise == null)
            throw new Exception("finalise can not be null when UseFinalise is true");


        return new TaskFactory().StartNew(() =>
        {
            int tc = this.MaxParallel;
            bool exit = false;

            Task finaliseTask = null;
            if (this.UseFinalise)
                finaliseTask = finaliseProcess(finalise);

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
                        state = _threadsState.First(a => !a.Allocated);
                        state.Allocated = true;
                    }
                    state.Object = _q.Dequeue(); //dequeue item and allocate
                    state.Index = Interlocked.Read(ref _fc);
                    Interlocked.Increment(ref _fc);
                    Monitor.Pulse(_lq); //allow another item to be queued
                }

                ThreadPool.QueueUserWorkItem(obj =>
                {
                    threadState ts = (threadState)obj;
                    try
                    {
                        process((T)ts.Object, (int)ts.ThreadIndex);
                    }
                    finally
                    {
                        if (this.UseFinalise)
                            finaliseObject(ts);
                        else
                            completeProcessing(ts);
                    }
                }, state);
            }

            while (true)
            {
                lock (_lp)
                {
                    if (_tc == 0)
                    {
                        while (this.UseFinalise && !finaliseTask.IsCompleted)
                        {
                            lock (_lf)
                                Monitor.PulseAll(_lf);
                            finaliseTask.Wait();
                        }
                        return;
                    }
                    lock (_lp)
                        Monitor.Pulse(_lp);
                }
            }

        });
    }

    private void completeProcessing(threadState state)
    {
        lock (_lp)
        {
            state.Object = null; //release
            state.Allocated = false;
            _tc--; //complete
            Monitor.Pulse(_lp); //allow another thread to go
        }
    }

    private void finaliseObject(threadState ts)
    {
        lock (_lf) //gain a lock - finaliseProcess should already be waiting
        {
            finaliseState fts = _finaliseState.First(a => a.ThreadState == null);
            fts.ThreadState = ts;
            Monitor.Pulse(_lf); //restart it
        }
    }

    private Task finaliseProcess(Action<T> finalise)
    {
        return new TaskFactory().StartNew(() =>
        {
            while (!_complete || _fp != Interlocked.Read(ref _fc))
            {
                finaliseState fts = null;
                lock (_lf)
                {
                    fts = _finaliseState.FirstOrDefault(a => a.ThreadState?.Index == _fp);
                    if (fts == null)
                        Monitor.Wait(_lf);
                    Monitor.Pulse(_lf);
                }
                if (fts != null)
                {
                    _fp++;
                    threadState ts = fts.ThreadState;
                    finalise((T)fts.ThreadState.Object);
                    lock (_lf)
                        fts.ThreadState = null;
                    completeProcessing(ts);
                }
            }
        });
    }
}
