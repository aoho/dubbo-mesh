package com.alibaba.dubbo.performance.demo.agent.transport;

/**
 * @author 徐靖峰
 * Date 2018-05-25
 *
 * 点对点的通信
 */
public interface Client{

    void init();

    MeshChannel getChannel();

}
