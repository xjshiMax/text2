# -*- coding:utf-8 -*-
DROP DATABASE IF EXISTS `GElog`; -- 不存在则创建
CREATE DATABASE `GElog`;
use GElog;
-- 创建一张 ip 与 国家的一个映射表

DROP TABLE  IF EXISTS `ipmapping`;

CREATE TABLE `ipmapping`
(
  `ipDomain`  VARCHAR(255) NOT NULL PRIMARY KEY, -- ip地址
  `ipcountry` VARCHAR(255)  NOT NULL,            -- ip地址所对应的国家
  `des`       TEXT                               -- ip地址的描述信息
)default charset=utf8;

-- 插入一些常见的IP地址

INSERT INTO ipmapping (ipDomain, ipcountry, des) VALUES ('0.0.0.0','Error','这是IP地址的中的保留地址，常作为源地址');
INSERT INTO ipmapping (ipDomain, ipcountry, des) VALUES ('8.8.8.8','美国','这是一个美国的免费的DNS服务器的地址');
INSERT INTO ipmapping (ipDomain, ipcountry, des) VALUES ('127.0.0.1','本地地址','这是本机器的回环网络地址，常做测试使用');
INSERT INTO ipmapping (ipDomain, ipcountry, des) VALUES ('255.255.255.255','广播地址','这是广播地址，不参与通信');
