package com.hmdp.utils;

public interface Ilock {
    public boolean tryLock(long expireTimeSec);
    public void unlock();
}
