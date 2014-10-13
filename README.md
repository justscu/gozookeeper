（1）使用“samuel/go-zookeeper/”作为客户端，并对其进行二次封装；
（2）添加了读写安全的map；
（3）最终使用的功能，如函数GetZookeeperImage。
（4）可以在GetZookeeperImage中加入go routinue，并加入信号。这样可以起到一直监控的效果。
