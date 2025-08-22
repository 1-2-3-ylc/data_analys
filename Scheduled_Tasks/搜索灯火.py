from operator import index

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
# import seaborn as sns
#sns.set(style="darkgrid")
plt.rcParams["font.sans-serif"]=["SimHei"]
plt.rcParams["axes.unicode_minus"]=False


from dateutil.relativedelta import relativedelta
from datetime import datetime as dt
import time

import warnings
warnings.filterwarnings("ignore")

import sys
import pymysql
from sqlalchemy import create_engine
import json
from datetime import datetime, timedelta , timezone
import time
import re
from Class_Model.All_Class import Data_Clean, All_Model

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler

pd.set_option('display.max_columns', None)

class Ssdh:
    def __init__(self):
        self.clean = Data_Clean()
        self.all_models = All_Model()
    # 连接数据库
    def query(self, sql,
              host="rm-wz930e5269fur1ht1mo.mysql.rds.aliyuncs.com",
              user="wxz",
              password="5JRcY9SaiepVlIq7iuPo",
              database='',
              port=3306
              ):
        conn = pymysql.connect(
            host=host,
            user=user,
            port=port,
            password=password,
            max_allowed_packet=1073741824,
            charset="utf8")
        try:
            df = pd.read_sql(sql, con=conn)
            conn.close()
        except:
            print('error')
            conn.close()
            raise
        return df

    # 查询数据
    def select_data(self):
        sql1 = ''' -- 订单&风控信息  近10日数据   
        SELECT date(om.create_time) as create_date,om.create_time,om.id as order_id ,om.order_number,om.all_money 
        ,om.status
        ,case om.`status` when  1 then "待支付" when  2 then "待发货" when  3 then "待收货" when  4 then "租赁中" when  5 then "待归还" 
        when  6 then "待商家收货" when  7 then "退押中" when  8 then "已完成" when  10 then "已退款" when  11 then "待退押金" when  12 then "待审核" 
        when  13 then "订单取消" when  15 then "检测中" when  9999 then "逾期订单" end as status2 
        ,case when locate('租物',pa.name)>0 or locate('租物',cc.name)>0 or locate('芝麻',pa.name)>0 or locate('芝麻',cc.name)>0  then '芝麻租物' when locate('抖音',pa.name)>0 then '抖音渠道' when locate('搜索',cc.name)>0 then '搜索渠道' else '其他渠道' end as channel_type 
        ,tod.sku_attributes,tod.product_name,tod.new_actual_money
        ,case when  locate('租完即送',tod.sku_attributes)>0 then '租完即送' else '租物归还' end as back_type
        ,om.user_mobile,tmu.true_name,tmu.id_card_num
        ,top.total_describes,tor.decision_result,om.cancel_reason
        ,replace(case when JSON_VALID(tor.decision_result) THEN JSON_EXTRACT(tor.decision_result, '$.traceid') end,'"','') as trace_id 
        ,replace(case when JSON_VALID(tor.decision_result) THEN JSON_EXTRACT(tor.decision_result, '$.rejected') end,'"','') as rejected 
        ,replace(case when JSON_VALID(tor.decision_result) THEN JSON_EXTRACT(tor.decision_result, '$.result') end,'"','') as result 
        ,replace(case when JSON_VALID(tor.decision_result) THEN JSON_EXTRACT(tor.decision_result, '$.tips') end,'"','') as tips 
        ,replace(case when JSON_VALID(tor.risk) THEN JSON_EXTRACT(tor.risk, '$.is_vip') end,'"','') as is_vip
        ,replace(case when JSON_VALID(tor.risk) THEN JSON_EXTRACT(tor.risk, '$.status') end,'"','') as status_result
        ,replace(case when JSON_VALID(tor.risk) THEN JSON_EXTRACT(tor.risk, '$.queue_verify_type') end,'"','') as qvt_risk
        ,replace(case when JSON_VALID(tor.decision_result) THEN JSON_EXTRACT(tor.decision_result, '$.queue_verify_type') end,'"','') as qvt_result 
        ,cc.name as channel_name         -- 来源渠道
        ,pa.name as activity_name        -- 活动名称
        ,om.merchant_id,om.merchant_name
        ,topay.total_freeze_fund_amount 
        ,om.buy_service_product,tso.status as service_status 
        ,om.order_method, tor.update_time, tomt.reason, cc.channel_type_id, om.order_type
        from  db_digua_business.t_order  om

        left join db_digua_business.t_order_risk tor on om.id = tor.order_id
        -- 备注信息合并 
        left join ( SELECT  t.order_id,JSON_ARRAYAGG(t.describes) as total_describes from db_digua_business.t_order_personnel t   GROUP BY 1 ) top 
        on om.id = top.order_id 
        -- 服务信息
        left join  db_digua_business.t_service_order tso  on om.id = tso.order_id 
        -- 渠道名称
        left join db_digua_business.t_channel cc on om.channel = cc.scene 
        -- 活动名称
        left join db_digua_business.t_platform_activity pa on om.activity_id = pa.id
        -- 用户信息 
        left join db_digua_business.t_member_user tmu on om.user_id = tmu.id
        -- 商品信息
        left join db_digua_business.t_order_details tod on om.id = tod.order_id
        -- 免押信息  
        left join (SELECT t.*,row_number() over(partition by t.order_id order by t.pay_date desc) as rn 
        from db_digua_business.t_order_pay t 
        where t.pay_type = 'ZFBYSQ' and t.item_type=1 and t.`status` in (2,5) and t.trade_no is not null )  topay 
        on topay.order_id=om.id   and  topay.rn = 1 
        -- 商家订单转移表
        left join db_digua_business.t_order_merchant_transfer tomt on tomt.order_id=om.id
        where om.user_mobile is not null 
        and tmu.true_name not in ("刘鹏","谢仕程","潘立","洪柳","陈锦奇","周杰","卢腾标","孔靖","黄娟","钟福荣","邱锐杰","唐林华"
        ,"邓媛斤","黄子南","刘莎莎","赖瑞彤","孙子文",'张娜','罗文龙','孔靖','彭康力','何薪华','夏玥','潘佳','包闻天','方全龙','李楠','向圆圆','黄兰娟','林婉婷','廖丽敏','李巧玲','李巧凤','刘三妹','蔡斯静'
        ,'陈宜诗','陈宝易','林寅钗','谢金凤','刘宏生','骆昌鑫','何静')
        and  om.create_time >= DATE_ADD(CURRENT_DATE,INTERVAL -1 month)               -- 近1个月数据
        ;
        '''
        sql3 = '''
        SELECT risk_trace_id trace_id, id_card, time, replace(case when JSON_VALID(data) THEN JSON_EXTRACT(data, '$.status') end,'"','') as status   FROM `db_credit`.risk_examine
        '''
        sql_risk = ''' -- risk等级
                select trace_id, id_card as id_card_r, time, replace(case when JSON_VALID(data) THEN JSON_EXTRACT(data, '$.status') end,'"','') as status_r from db_credit.risk
                '''
        df_risk = self.query(sql_risk)
        sql_ra = ''' -- 996强拒表
                select order_id, time, replace(case when JSON_VALID(result) THEN JSON_EXTRACT(result, '$.status') end,'"','') as status_ra  from db_credit.risk_alipay_interactive_prod_result
                '''
        df_ra = self.query(sql_ra)
        df_ra.loc[:, 'time_ra'] = pd.to_datetime(df_ra.time.dt.date, errors="coerce")
        df_ra = df_ra.sort_values(by='time', ascending=False).groupby('order_id').head(1)

        df_risk_examine = self.query(sql3)
        df = self.query(sql1)
        df_re = df_risk_examine.copy()
        df_re.loc[:, 'time_re'] = pd.to_datetime(df_re.time.dt.date, errors="coerce")
        df_re.rename(columns={'id_card': 'id_card_re', 'status': 'status_re'}, inplace=True)

        return df, df_risk_examine, df_risk, df_re, df_ra


    # 获取颜色
    def getcolor(self, s):
        color_list = json.loads(s)
        for j in range(0, len(color_list)):
            if color_list[j]["key"] == "颜色":
                return color_list[j]["value"]

    # 获取内存
    def getneicun(self, s):
        color_list = json.loads(s)
        for j in range(0, len(color_list)):
            if color_list[j]["key"] == "内存":
                return color_list[j]["value"]

    # 数据清理
    def clean_data(self, df, df_risk, df_re, df_ra):
        # 处理日期
        df["下单日期"] = df["create_time"].dt.date
        df["下单日期"] = pd.to_datetime(df["下单日期"], errors="coerce")
        df["月份"] = df["下单日期"].dt.month
        # 获取原因，理由
        df['拒绝理由'] = df["rejected"].str.replace("[", "").str.replace("]", "").str.replace('"', '')
        df = df[df['sku_attributes'].notnull()]
        df["取消原因"] = df["total_describes"].str.split("客户申请取消：").str[1].str.split("$").str[0].str.strip()
        df["电审拒绝原因"] = df["total_describes"].str.split("审核不通过：").str[1].str.split("$").str[0].str.strip()
        df["无法联系原因"] = df["total_describes"].str.split("用户无法联系：").str[1].str.split("$").str[0].str.strip()
        # 获取商品类型和方案类型
        df.loc[:, "商品类型"] = np.where(
            df["product_name"].str.contains('99新') | df["product_name"].str.contains('95新') | df[
                "product_name"].str.contains('准新'), "二手", "全新")  ##  准新
        df.loc[:, "租赁方案"] = np.where(df["sku_attributes"].str.contains('租完即送'), "租完即送", "租完归还")
        df.loc[:, "押金类型"] = np.where(df["total_freeze_fund_amount"] > 0, "部分免押", "全免押")
        df.loc[:, "优惠券使用否"] = np.where(
            (df["new_actual_money"] - df["all_money"] > 0) & (df["租赁方案"] == '租完即送'), "已使用", "未使用")
        # 判断是否进件和定义渠道
        df.loc[:, "是否进行预授权"] = np.where(df["total_freeze_fund_amount"].isnull(), "未预授权", "已预授权")
        df.loc[:, "内存"] = df.apply(lambda x: self.getneicun(x["sku_attributes"]), axis=1)
        df.loc[:, "进件"] = np.where((df["status2"] == "待支付") | (df["status2"] == "订单取消"), "未进件", "进件")
        df.loc[:, "来源渠道"] = df["channel_name"].fillna("未知渠道")
        # 获取免人审数据
        df['免审'] = np.where(df.decision_result.str.contains(pat='免人审', regex=False), 1, 0)
        # 渠道归属
        df.loc[:, "归属渠道"] = df.apply(lambda x: self.clean.qudao_type(x["来源渠道"], x["activity_name"], x["order_method"], x['channel_type_id'], x['order_type']),axis=1)

        # 订单去重
        dict_status_code = {
            "订单取消": 1,
            "待支付": 2,
            "已退款": 3,
            "待审核": 4,
            "待发货": 5,
            "待收货": 6,
            "租赁中": 7,
            "已完成": 8
        }
        df["状态编码"] = df["status2"].map(dict_status_code)

        df.sort_values(by=["下单日期", "状态编码"], inplace=True)

        # 删除重复单号
        df.drop_duplicates(subset=["order_id"], inplace=True)
        # 删除身份证空值行
        df.dropna(subset=["id_card_num"], axis=0, inplace=True)
        # 去刷单订单
        df.drop(df[df['total_describes'].str.contains(pat='panli', regex=False) == True].index, inplace=True)
        df.drop(df[df['activity_name'] == "1000单秘密计划"].index, inplace=True)
        df.drop(df[df['activity_name'] == "1000单秘密计划-无优惠"].index, inplace=True)
        df.drop(df[df['activity_name'] == "1000单曙光计划"].index, inplace=True)
        df.drop(df[df['activity_name'] == "线下门店3个月试行"].index, inplace=True)
        # 删除身份证空值行
        df.dropna(subset=["status2"], axis=0, inplace=True)

        # 删除重复订单
        df.drop_duplicates(subset=["order_id"], inplace=True)
        df.drop_duplicates(subset=["true_name", "user_mobile", "id_card_num", "下单日期"], keep="last", inplace=True)

        df.drop(df[df['true_name'].isin(
            ["刘鹏", "谢仕程", "潘立", "洪柳", "陈锦奇", "周杰", "卢腾标", "孔靖", "黄娟", "钟福荣", "邱锐杰", "唐林华"
                , "邓媛斤", "黄子南", "刘莎莎", "赖瑞彤", "孙子文"])].index, inplace=True)

        # 定义状态
        df = df.merge(df_risk[['trace_id', 'status_r']], on='trace_id', how='left').merge(
            df_re[['trace_id', 'status_re']], on='trace_id', how='left').merge(
            df_ra[['order_id', 'time_ra', 'status_ra']], left_on=['order_id', '下单日期'],
            right_on=['order_id', 'time_ra'], how='left')
        df['是否前置拦截'] = np.where(
            (df.result.str.contains('id_card不得为空')) | (df.result.str.contains('mobile校验不通过')) | (
                df.result.str.contains('name校验不通过'))
            | (df.result.str.contains('年龄超过49岁或低于18岁')) | (df.result == '风控未通过') | (
                        df.result == '黑名单用户'), 1, 0)
        df['是否机审强拒'] = np.where(df.status_r == '1', 1, 0)
        df['是否出库前风控强拒'] = np.where((df.status_re == '1') | (df.status_ra == '1'), 1, 0)
        # 定义审核状态
        df.loc[:, "审核状态"] = df.apply(
            lambda x: self.clean.reject_type(x["拒绝理由"], x["进件"], x["电审拒绝原因"], x["取消原因"], x["status2"],
                                  x["无法联系原因"], x["total_describes"], x['是否前置拦截'], x['是否机审强拒'],
                                  x['是否出库前风控强拒']), axis=1)
        df["下单时段"] = df["create_time"].astype(str).str[:14]
        self.df_j = df[df["进件"] == "进件"]
        # 判断设置各个节点状态
        df["待审核"] = np.where(df["审核状态"] == '待审核', 1, 0)
        df["前置拦截"] = np.where(df["审核状态"] == '前置拦截', 1, 0)
        df["人审拒绝"] = np.where(df["审核状态"] == '人审拒绝', 1, 0)
        df["客户取消"] = np.where(df["审核状态"] == '客户取消', 1, 0)
        df["无法联系"] = np.where(df["审核状态"] == '无法联系', 1, 0)
        df["是否进件"] = np.where(df["进件"] == '进件', 1, 0)
        df["是否出库"] = np.where(df["status"].isin([2, 3, 4, 5, 6, 8, 15]), 1, 0)

        df["进件前取消"] = np.where(df["审核状态"] == '进件前取消', 1, 0)
        df['是否出库'] = np.where((df['人审拒绝']==0)&(df['客户取消']==0)&(df['无法联系']==0)&(df['待审核']==0)&(df['是否出库']==1), 1, 0)
        df["出库前风控强拒"] = np.where((df["审核状态"]=='出库前风控强拒')&(df['是否出库']==0),1,0)
        df["机审强拒"] = np.where((df["审核状态"]=='机审强拒')&(df['是否出库']==0),1,0)
        df['机审通过件'] = np.where((df['是否进件']==1)&(df['机审强拒']==0), 1, 0)
        df['风控通过件'] = np.where((df['是否进件']==1)&(df['机审强拒']==0)&(df['人审拒绝']==0), 1, 0)

        df['已退款'] = np.where((df['风控通过件']==1)&(df['审核状态']=='已退款'), 1, 0)

        self.df_cks = df[df["是否出库"] == 1]
        return df

    # 获取台账数据
    def data_tz(self, df):
        # 读取导入出库台账数据
        f_path_ck = "F:/myfile/p站数据/台账数据/维客壹佰2023&2024年台账.xlsx"
        df_ck = pd.read_excel(f_path_ck, sheet_name="2023")
        df_ck['溢价费订单'] = "A" + df_ck[df_ck['备注'].str.contains(pat='溢价费', regex=False) == True][
            '备注'].str.extract('(\d+.\d+)')
        df_yijia = df_ck[df_ck['溢价费订单'].str.len() >= 16][['溢价费订单', "已付金额"]]
        df_yijia = df_yijia.rename(columns={'溢价费订单': "单号", "已付金额": "溢价费"})
        df_yijia_ck = pd.merge(df_ck, df_yijia, left_on="订单号", right_on="单号", how="left")
        dfck = pd.merge(df_yijia_ck, df, left_on="订单号", right_on="order_number")

        dfck.drop_duplicates(subset=["订单号"], inplace=True)
        dfck.drop(dfck[dfck["status2"] == "已退款"].index, inplace=True)
        # 删除 露营设备 出库
        dfck.drop(dfck[dfck["类目"] == "露营设备"].index, inplace=True)

        return dfck

    # 获取分组数据
    def get_data_group(self, df, df_risk_examine):
        # 获取剔除拒量的数据
        df2 = df.copy()
        df2 = self.clean.drop_rejected_merchant(df2)
        # 获取高频词
        # df_gpc = df[df['来源渠道'] == '付费灯火-高频词']
        # df_gpc2 = df2[df2['来源渠道'] == '付费灯火-高频词']
        # df_gpc_group = self.all_models.data_group(df_gpc, df_gpc2, df_risk_examine, '下单日期')[['去重订单数', '前置拦截', '拦截率', '进件数', '预授权通过率', '机审强拒', '强拒比例', '机审通过件', '人审拒绝', '风控通过件', '风控通过率'
        #         , '客户取消', '无法联系', '出库前风控强拒', '待审核', '出库', '进件出库率', '订单出库率']]
        # 付费侠客行-苹果旗舰
        df_apple = df[df['归属渠道'] == '付费侠客行-苹果旗舰']
        df_apple2 = df2[df2['归属渠道'] == '付费侠客行-苹果旗舰']
        df_apple_group = self.all_models.data_group(df_apple, df_apple2, df_risk_examine, '下单日期')[['去重订单数', '前置拦截', '拦截率', '进件数', '预授权通过率', '机审强拒', '强拒比例', '机审通过件', '人审拒绝', '风控通过件', '风控通过率'
                , '客户取消', '无法联系', '出库前风控强拒', '待审核', '出库', '进件出库率', '订单出库率']]
        # 付费邦道 - 苹果旗舰
        # df_apple_qj = df[df['归属渠道'] == '付费邦道-苹果旗舰']
        # df_apple_qj2 = df2[df2['归属渠道'] == '付费邦道-苹果旗舰']
        # df_apple_qj2_group = self.all_models.data_group(df_apple_qj, df_apple_qj2, df_risk_examine, '下单日期')[['去重订单数', '前置拦截', '拦截率', '进件数', '预授权通过率', '机审强拒', '强拒比例', '机审通过件', '人审拒绝', '风控通过件', '风控通过率'
        #         , '客户取消', '无法联系', '出库前风控强拒', '待审核', '出库', '进件出库率', '订单出库率']]
        # 付费创本-搜索
        df_ffcb = df[df['来源渠道'] == '付费创本-搜索']
        df_ffcb2 = df2[df2['来源渠道'] == '付费创本-搜索']
        df_ffcb_group = self.all_models.data_group(df_ffcb, df_ffcb2, df_risk_examine, '下单日期')[
            ['去重订单数', '前置拦截', '拦截率', '进件数', '预授权通过率', '机审强拒', '强拒比例', '机审通过件',
             '人审拒绝', '风控通过件', '风控通过率'
                , '客户取消', '无法联系', '出库前风控强拒', '待审核', '出库', '进件出库率', '订单出库率']]

        return df_apple_group, df_apple2, df_ffcb_group#, df_apple_qj2_group#, df_sy_group

    def fp(self):
        # 进件
        self.df_j['订单号'] = self.df_j['order_number']
        self.df_j['商品型号'] = self.df_j['product_name']
        self.df_j['买断金额'] = self.df_j['new_actual_money']
        self.df_j['总租金'] = self.df_j['all_money']
        self.df_j['订单状态'] = self.df_j['status2']
        # 出库
        self.df_cks['订单号'] = self.df_cks['order_number']
        self.df_cks['商品型号'] = self.df_cks['product_name']
        self.df_cks['总租金'] = self.df_cks['all_money']
        self.df_cks['订单状态'] = self.df_cks['status2']
        self.df_cks['买断金额'] = self.df_cks['new_actual_money']
        # 高频词
        # 搜索灯火高频词进件明细
        df_ssdhj_gpc = self.df_j[self.df_j["来源渠道"] == "付费灯火-高频词"][
            ["下单日期", "订单号", "商品型号", "总租金", "买断金额", "租赁方案", "来源渠道", "订单状态", "审核状态","拒绝理由", "取消原因", "电审拒绝原因"]]
        # 搜索灯火高频词出库明细,阿钗需求
        df_ssdhck_gpc = self.df_cks[self.df_cks["来源渠道"] == "付费灯火-高频词"][
            ["下单日期", "订单号", "商品型号", "总租金", "买断金额", "租赁方案", "来源渠道"]]

        # 付费邦道-首页进件明细
        df_ffbdsy = self.df_j[self.df_j["归属渠道"] == "付费邦道-首页"][
            ["下单日期", "订单号", "商品型号", "总租金", "买断金额", "租赁方案", "来源渠道"]]
        # 付费邦道-首页出库明细
        df_ffbdsyck = self.df_cks[self.df_cks["归属渠道"] == "付费邦道-首页"][
            ["下单日期", "订单号", "商品型号", "总租金", "买断金额", "租赁方案", "来源渠道"]]
        # 邦道-自定义进件明细
        df_bdzdy = self.df_j[self.df_j["归属渠道"] == "邦道-自定义"][
            ["下单日期", "订单号", "商品型号", "总租金", "买断金额", "租赁方案", "来源渠道"]]
        # 邦道-自定义出库明细
        df_bdzdyck = self.df_cks[self.df_cks["归属渠道"] == "邦道-自定义"][
            ["下单日期", "订单号", "商品型号", "总租金", "买断金额", "租赁方案", "来源渠道"]]
        # 邦道-销售线索进件明细
        df_xsxs = self.df_j[self.df_j["归属渠道"] == "邦道-销售线索"][
            ["下单日期", "订单号", "商品型号", "总租金", "买断金额", "租赁方案", "来源渠道"]]
        # 邦道-销售线索出库明细
        df_xsxsck = self.df_cks[self.df_cks["归属渠道"] == "邦道-销售线索"][
            ["下单日期", "订单号", "商品型号", "总租金", "买断金额", "租赁方案", "来源渠道"]]
        # 付费侠客行-苹果旗舰进件明细
        df_xkx = self.df_j[self.df_j["归属渠道"] == "付费侠客行-苹果旗舰"][
            ["下单日期", "订单号", "商品型号", "总租金", "买断金额", "租赁方案", "来源渠道"]]
        # 付费侠客行-苹果旗舰出库明细
        df_xkxck = self.df_cks[self.df_cks["归属渠道"] == "付费侠客行-苹果旗舰"][
            ["下单日期", "订单号", "商品型号", "总租金", "买断金额", "租赁方案", "来源渠道"]]

        return df_ssdhj_gpc, df_ssdhck_gpc, df_ffbdsy,df_ffbdsyck, df_bdzdy, df_bdzdyck, df_xsxs, df_xsxsck, df_xkx, df_xkxck



    def run(self):
        print('正在查询数据...')
        df1, df_risk_examine, df_risk, df_re, df_ra = self.select_data()
        print('数据查询完毕...\n正在进行数据清理...')
        df2 = self.clean_data(df1, df_risk, df_re, df_ra)
        print('数据清理完毕...')
        dfck = self.data_tz(df2)
        print('正在获取数据分组...')
        df_apple_group, df_apple2, df_ffcb_group = self.get_data_group(df2, df_risk_examine) #, df_gpc_group, df_sy_group
        print('数据获取完毕...\n准备执行定时任务...')
        return df_apple_group, df_apple2, df_ffcb_group#, df_apple_qj2_group
        # self.my_job(self.hour, self.minute, self.path, self.Today, df_gpc_group, df_apple_group, df_sy_group)
        # df_ssdhj_gpc, df_ssdhck_gpc, df_ffbdsy,df_ffbdsyck, df_bdzdy, df_bdzdyck, df_xsxs, df_xsxsck, df_xkx, df_xkxck =self.fp()



    def my_job(self, hour, minute, path, Today):
        df_apple_group, df_apple2,df_ffcb_group = self.run() # , df_gpc_group, df_sy_group
        print(f'执行定时任务：现在是每日的{hour}:{minute}')
        # with pd.ExcelWriter(path + f'搜索灯火_{Today}.xlsx', engine='openpyxl') as writer:
        #     df_gpc_group.to_excel(writer, sheet_name='高频词')
        with pd.ExcelWriter(path + f'付费创本_{Today}.xlsx', engine='openpyxl') as writer:
            df_ffcb_group.to_excel(writer, sheet_name='付费创本-搜索')
        # with pd.ExcelWriter(path + f'付费邦道_{Today}.xlsx', engine='openpyxl') as writer:
        #     df_apple_qj2_group.to_excel(writer, sheet_name='付费邦道-苹果旗舰')
        with pd.ExcelWriter(path + f'付费侠客行{Today}.xlsx', engine='openpyxl') as writer:
            df_apple_group.to_excel(writer, sheet_name='付费侠客行-苹果旗舰')
            df_apple2[['下单日期', 'order_number', 'rejected']].to_excel(writer, sheet_name='付费侠客行-苹果旗舰_拒绝原因', index=False)

if __name__ == '__main__':
    hour = 9
    minute = 30
    path = r'\\digua\迪瓜租机\20.搜索灯火/'
    Today = str(datetime.now().strftime('%Y%m%d%H'))
    ssdh = Ssdh()
    print('正在创建定时任务...')
    scheduler = BackgroundScheduler()
    job = scheduler.add_job(ssdh.my_job, 'cron', hour=hour, minute=minute, args=[hour, minute, path, Today])
    print('定时任务创建完毕...\n正在执行定时任务my_job...')
    print(scheduler.get_jobs())
    scheduler.start()
    # 模拟主程序
    try:
        while True:
            next_run_time = job.next_run_time
            if next_run_time:
                now = datetime.now(timezone.utc)
                sleep_duration = (next_run_time - now).total_seconds()

                if sleep_duration > 0:
                    time.sleep(sleep_duration)
            else:
                time.sleep(60)  # 如果没有找到下次运行时间，则等待一段时间后重新检查
    except (KeyboardInterrupt, SystemExit):
        # 用户按下 Ctrl+C 或系统要求退出时，优雅地关闭调度器
        scheduler.shutdown()
