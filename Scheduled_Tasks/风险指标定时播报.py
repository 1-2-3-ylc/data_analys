import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pandas._libs.tslibs.offsets import MonthEnd

plt.rcParams["font.sans-serif"]=["SimHei"]
plt.rcParams["axes.unicode_minus"]=False
import warnings
warnings.filterwarnings("ignore")
import requests
import hmac
import hashlib
import base64
import pymysql
from datetime import timedelta, datetime, timezone
import time
from apscheduler.schedulers.background import BackgroundScheduler
from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"
from Class_Model.All_Class import All_Model, Data_Clean, Risk_Data

class Risk_Indicator:
    def __init__(self):
        self.all_models = All_Model()
        self.clean = Data_Clean()
        self.risk = Risk_Data()
        # 替换为你的 Webhook 地址
        self.webhook = "https://oapi.dingtalk.com/robot/send?access_token=b52ba60d13752496c9c15b7a3c78340a5bbde4150534ab2c023134ee9f5d8c42"
        # 替换为你的密钥，如果没有设置则留空
        self.secret = "SECb65f04f65b25f27dad132e6116a1806f14fee601ef4e9a36593c0ec277135657"


    def select_data(self):
        sql = '''-- 租后应收监控  
        SELECT  tprm.* 
        ,om.`status` as 订单状态值
        ,case om.`status` when  1 then "待支付" when  2 then "待发货" when  3 then "待收货" when  4 then "租赁中" when  5 then "待归还" 
        when  6 then "待商家收货" when  7 then "退押中" when  8 then "已完成" when  10 then "已退款" when  11 then "待退押金" when  12 then "待审核" 
        when  13 then "订单取消" when  15 then "检测中" when  9999 then "逾期订单" end as  订单状态
        ,om.order_finish_date as 订单完成时间,om.order_method
        ,tmu.true_name ,tmu.id_card_num,tmu.mobile 
        ,tod.product_name

        -- 2025.4.29
        ,cc.name as channel_name_cc         -- 来源渠道
        ,cc.channel_type_id              -- 渠道id
        ,pa.name as activity_name        -- 活动名称
        ,pa.type
        ,om.order_type,tod.sku_attributes
        ,replace(case when JSON_VALID(tor.decision_result) THEN JSON_EXTRACT(tor.decision_result, '$.tips') end,'"','') as tips  
        ,replace(case when JSON_VALID(tor.risk) THEN JSON_EXTRACT(tor.risk, '$.is_vip') end,'"','') as is_vip
        ,replace(case when JSON_VALID(tor.risk) THEN JSON_EXTRACT(tor.risk, '$.status') end,'"','') as status_result

        from db_digua_business.t_postlease_receivables_monitoring   tprm 
        left join db_digua_business.t_order om on tprm.order_id = om.id
        left join  db_digua_business.t_member_user tmu on om.user_id = tmu.id
        left join  db_digua_business.t_order_details tod on tprm.order_id = tod.order_id

        -- 2025.4.29
        left join db_digua_business.t_order_risk tor on om.id = tor.order_id
        -- 渠道名称
        left join db_digua_business.t_channel cc on om.channel = cc.scene 
        -- 活动名称
        left join db_digua_business.t_platform_activity pa on om.activity_id = pa.id
        ;
        '''
        df_zhys = self.clean.query(sql)
        df_zhys = self.clean.drop_merchant(df_zhys)
        df_zhys = self.clean.drop_rejected_merchant(df_zhys)
        # 买断数据
        sql1 = ''' --   买断信息 
        with out_order as ( 
        SELECT too.order_id,too.`status` 
        ,too.real_pay_money ,too.create_time,too.update_time
        ,too.pay_date ,too.actual_money 
        ,rank() over(partition by too.order_id order by too.pay_date desc) as rn 
        from db_digua_business.t_order_out too
        where  too.`status` not in (1)   -- 买断状态：1 未买断 2 已买断  3 部分买断 
        )
        SELECT distinct t1.*
        ,t2.`status`
        ,case when t2.`status`=2 then '已买断' when  t2.`status`=3 and om.order_finish_date is not null then '已买断'  else '部分买断' end as if_outpay 
        ,om.order_finish_date
        ,case when t2.`status`=2 and om.order_finish_date is null then t1.pay_date else om.order_finish_date end as finish_date_new 
        from (
        SELECT too.order_id 
        ,sum(too.real_pay_money) as outpay_money
        ,max(too.pay_date) as pay_date 
        from out_order  too
        GROUP BY 1 -- ,2
        ORDER BY 2 desc ,1) t1 
        left join out_order t2 on t1.order_id = t2.order_id and t2.rn=1 
        left join db_digua_business.t_order om on t1.order_id = om.id 
        ;
        '''
        df_out = self.clean.query(sql1)

        # 获取分期取消数据
        sql1 = '''-- 订单分期表  取消的分期
        SELECT tos.order_id,count(*) as nn
        ,count(tos.reality_refund_date) as  reality_refund_date
        ,sum(tos.real_pay_money) as real_pay_money
        ,sum(tprm.purchase_amount)/count(tos.order_id) as purchase_amount
        ,avg(tprm.advance_periods) as  advance_periods
        ,avg(tprm.advance_sum) as advance_sum
        from db_digua_business.t_order_stages tos 
        inner join db_digua_business.t_postlease_receivables_monitoring   tprm 
        on tos.order_id = tprm.order_id 
        # 订单取消
        where tos.`status`=4 
        GROUP BY 1 order by 2 desc 
        ;
        '''
        df_cancel = self.clean.query(sql1)

        # 账期数据（含续租）
        sql_xz = ''' 
        -- 状态,1:未支付  2:扣款失败 3:已支付 4：已取消 5、申请扣款、6已退款',
        -- '是否续租账期,0非续租账期，1预授权账期，2自定义续租账期',
        SELECT ymos.*
        from db_rent.ya_merchant_order_stages ymos 
        inner join db_digua_business.t_postlease_receivables_monitoring tprm 
        on ymos.order_id = tprm.order_id 
        ;
        '''
        df_xzfq = self.clean.query(sql_xz)
        # 分期表
        sql = '''-- 租后应收监控  
        SELECT distinct hk.order_number, tos.order_id, tos.refund_date as refund_date_1
        from db_digua_business.t_order_stages tos
        inner join
            (select distinct om.id as order_id, om.order_number
            from db_digua_business.t_order om
            left join db_digua_business.t_platform_activity tpa 
            on tpa.id=om.activity_id
            where tpa.type = 4) hk
        on tos.order_id = hk.order_id
        '''
        tmp_hk = self.clean.query(sql)
        tmp_hk['refund_date_ym'] = tmp_hk['refund_date_1'].dt.strftime('%Y-%m')
        df_xzfq['refund_date_ym'] = df_xzfq['refund_date'].dt.strftime('%Y-%m')
        df_xzfq = df_xzfq.merge(tmp_hk[['order_id', 'refund_date_1', 'refund_date_ym']],
                                on=['order_id', 'refund_date_ym'], how='left')
        df_xzfq['refund_date_2'] = np.where(df_xzfq.refund_date_1.notnull(), df_xzfq.refund_date_1, df_xzfq.refund_date)
        df_xzfq.drop(columns=['refund_date_1', 'refund_date_ym', 'refund_date'], inplace=True)
        df_xzfq = df_xzfq.rename(columns={'refund_date_2': 'refund_date'})
        df_xzfq = df_xzfq.rename(columns={'money': '当前应付租金', 'part_payment': '实付金额', 'refund_date': '应付日期','reality_refund_date': '实付日期', 'sort': '当前期数'})

        # 更新逾期天数
        sql_xz = ''' 
        SELECT distinct tprm.order_number,1 as label
        # ,ymos.reality_refund_date,ymos.sort as '当前期数',ymos.status
        from db_rent.ya_merchant_order_stages ymos 
        inner join db_digua_business.t_postlease_receivables_monitoring tprm 
        on ymos.order_id = tprm.order_id 
        where ymos.sort=1 and ymos.reality_refund_date is null
        ;
        '''
        tmp = self.clean.query(sql_xz)
        return df_zhys, df_out, df_cancel, df_xzfq, tmp

    def clean_date(self, df_zhys, df_out, df_cancel, df_xzfq):
        # 1148 条历史订单渠道名称补充
        qudao_name_df = pd.read_excel('F:/myfile/租后数据/历史订单渠道名称补充.xlsx')
        qudao_name_df.drop(columns=['渠道id', '下单时间'], inplace=True)
        dfzh1 = df_zhys.merge(qudao_name_df, left_on='order_number', right_on='订单号', how='left')
        dfzh1["channel_name"] = np.where(dfzh1.渠道名称.notnull(), dfzh1.渠道名称, dfzh1["channel_name"])
        dfzh1.loc[:, "来源渠道"] = dfzh1["channel_name"].fillna("未知渠道")
        # 渠道归属
        dfzh1.loc[:, "归属渠道"] = dfzh1.apply(
            lambda x: self.clean.qudao_type(x["来源渠道"], x["activity_name"], x["order_method"], x['channel_type_id'],
                                            x['order_type']), axis=1)
        dfzh1['归属渠道'] = np.where(dfzh1.归属渠道 == '顶部搜索框的搜索结果页', '搜索渠道', dfzh1.归属渠道)
        conditions_1 = ['八派信息', 'CPS直播', '派金花', '九州信息', '勉丫租', '哈银', '租瓜直播', '美仑美奂',
                        '分期乐1', '分期乐2', '98租超', '推一推', '硬派抖音']
        dfzh1['归属渠道'] = np.where(dfzh1.归属渠道.isin(conditions_1), 'S量', dfzh1.归属渠道)
        dfzh1['是否免人审'] = np.where((dfzh1.is_vip == '1') & (dfzh1.status_result == '0'), 1, 0)
        dfzh1['是否拒量'] = np.where(dfzh1.tips.str.contains(r'策略2412|命中自有模型回捞策略|回捞策略250330命中') == True, 1, 0)
        dfzh1['是否号卡'] = np.where(dfzh1.type == 4, 1, 0)
        dfzh1["内存"] = dfzh1["specification"].str.split("内存：").str[1].str.split("颜色：").str[0].str.split(" ").str[0]
        dfzh1["颜色"] = dfzh1["specification"].str.split("内存：").str[1].str.split("颜色：").str[1].str.split(" ").str[0]
        dfzh1['是否二手'] = np.where(
            dfzh1['product_name'].str.contains('95新') | dfzh1['product_name'].str.contains('99新') | dfzh1[
                'product_name'].str.contains('准新'), "二手", "全新")
        # 日期处理
        dfzh1["下单日期"] = dfzh1['order_create_time'].dt.date
        dfzh1["下单日期"] = pd.to_datetime(dfzh1["下单日期"], errors="coerce")
        dfzh1["月份"] = dfzh1["下单日期"].dt.month
        dfzh1["年份"] = dfzh1["下单日期"].dt.year

        # 关联买断数据
        dfzh2 = dfzh1.merge(df_out[['order_id', 'outpay_money', 'pay_date', 'status', 'if_outpay']], on='order_id',how='left')
        dfzh2['finish_date_new'] = dfzh2.apply(
            lambda x: pd.to_datetime(x.pay_date, errors='coerce') if pd.isnull(x.order_finish_date) and x.status == 2
            else pd.to_datetime(x.订单完成时间, errors='coerce') if pd.isnull(x.order_finish_date) and pd.notnull(x.订单完成时间)
            else pd.to_datetime(x.order_finish_date, errors='coerce'), axis=1)
        dfzh2['if_outpay'] = dfzh2.apply(lambda x: '已买断' if x['status'] == 2
        else '已买断' if x['status'] == 3 and pd.notnull(x.finish_date_new)
        else '部分买断' if x['status'] == 3 and pd.isnull(x.finish_date_new)
        else '未知', axis=1)

        # 关联分期取消数据
        dfzh3 = dfzh2.merge(df_cancel[['order_id', 'nn', 'reality_refund_date', 'real_pay_money']], on='order_id',how='left')

        # 数据合并
        df_xz_concat = df_xzfq.merge(dfzh3, how='right', on='order_id')
        df_xz_concat.实付金额 = df_xz_concat.实付金额 + df_xz_concat.sesame_promo_money_pay + df_xz_concat.promo_money
        df_xz_concat = df_xz_concat[df_xz_concat.当前应付租金.notnull()]
        # 去重
        check = df_xz_concat[["order_id", "当前期数"]]
        all_duplicates = check[check.duplicated(keep=False)]
        # 删除重复订单
        df_xz_concat.drop_duplicates(subset=["order_id", "当前期数"], inplace=True)

        return df_xz_concat, dfzh3

    def mob_date(self,df):
        df['下单日期'] = pd.to_datetime(df['下单日期'])
        df['实付日期new'] = pd.to_datetime(df['实付日期new'])
        # note n 续租的没有统计
        for n in range(0, 13):
            # df[f'mob_date_{n}'] = df['下单日期'] + MonthEnd(n+1)
            # note 20241205 修改 月末的的跳级了，所以不要加1，mob0对齐
            # df[f'mob_date_{n}'] = np.where(df["下单日期"].dt.day.isin([28,29,30,31]), df['下单日期'] + MonthEnd(n), df['下单日期'] + MonthEnd(n+1))
            df[f'mob_date_{n}'] = np.where(
                (df["下单日期"].dt.day == 31) | (df["下单日期"].isin(['2023-02-28', '2024-02-29', '2025-02-28'])) | (
                            (df["下单日期"].dt.month.isin([4, 6, 9, 11])) & (df["下单日期"].dt.day == 30)),
                df['下单日期'] + MonthEnd(n), df['下单日期'] + MonthEnd(n + 1))
            df[f'mob_date_{n}'] = pd.to_datetime(df[f'mob_date_{n}'])
            df[f'paid_date_mob{n}'] = np.where(df['实付日期new'] > df[f'mob_date_{n}'], pd.NaT, df['实付日期new'])
            df[f'paid_money_mob{n}'] = np.where(df['实付日期new'] > df[f'mob_date_{n}'], np.nan, df['实付金额'])
            df[f'paid_date_mob{n}'] = pd.to_datetime(df[f'paid_date_mob{n}'])
        return df

    def update_date(self, df_xz_concat, tmp):
        df_xz_concat['实付日期'] = pd.to_datetime(df_xz_concat['实付日期']).dt.date
        df_xz_concat['finish_date_new'] = pd.to_datetime(df_xz_concat['finish_date_new']).dt.date
        # （1）部分还款 用户 实付日期非空 且非买断 非完成 ： 实付日期清空
        df_xz_concat['实付日期1'] = np.where(
            (df_xz_concat.实付金额 > 0) & (df_xz_concat.实付金额 < df_xz_concat.当前应付租金) & (
                df_xz_concat.实付日期.notnull()) & (df_xz_concat.finish_date_new.isnull()), pd.NaT,
            df_xz_concat.实付日期)
        # （2）已完成 还款 用户  实付日期 补充 订单完成时间       实付金额>0  实付日期为空  finish_date_new非空  :实付日期=订单完成时间finish_date_new
        df_xz_concat['实付日期1'] = np.where(
            (df_xz_concat.实付金额 > 0) & (df_xz_concat.实付日期1.isnull()) & (df_xz_concat.finish_date_new.notnull()),
            df_xz_concat.finish_date_new, df_xz_concat.实付日期1)
        # （3）租完即送用户 A2023081019051714 补还款时间
        df_xz_concat['实付日期1'] = np.where(
            (df_xz_concat.order_number == 'A2023081019051714') & (df_xz_concat.当前期数 == 12), '2024-07-22',
            df_xz_concat['实付日期1'])
        # （4）未还款 且 未完成 未取消用户 ： 实付日期清空
        df_xz_concat['实付日期1'] = np.where((df_xz_concat.实付金额 == 0) & (df_xz_concat.实付日期.notnull()) & (
            df_xz_concat.finish_date_new.isnull()) & (df_xz_concat.nn.isnull()), pd.NaT, df_xz_concat.实付日期1)
        # （5）实付为0，实付日期为空，但 完成时间非空，买断金额>0： 实付日期回写
        df_xz_concat['实付日期1'] = np.where((df_xz_concat.实付金额 == 0) & (df_xz_concat.实付日期1.isnull()) & (
            df_xz_concat.finish_date_new.notnull()) & (df_xz_concat.outpay_money > 0), df_xz_concat.finish_date_new,df_xz_concat.实付日期1)
        # （8）实付0，实付日期为空，但 完成时间非空：为取消订单，实付日期=完成时间
        df_xz_concat['实付日期new'] = np.where(
            (df_xz_concat.实付日期1.isnull()) & (df_xz_concat.finish_date_new.notnull()), df_xz_concat.finish_date_new,
            df_xz_concat.实付日期1)
        # （11）A202207050851373、A20241104144812228 应付日期修复
        df_xz_concat['应付日期'] = np.where((df_xz_concat.应付日期.isnull()) & (
            df_xz_concat.order_number.isin(['A202207050851373', 'A20241104144812228'])), pd.to_datetime('2023-07-05'),
                                            df_xz_concat.应付日期)

        # 获取逾期截止时间
        df_xz_concat['now_date'] = pd.Timestamp(datetime.now().date())
        # 重新定义状态
        conditions = [
            ((pd.isna(df_xz_concat['实付日期new'])) & (pd.isna(df_xz_concat['finish_date_new'])) & (
                df_xz_concat['订单状态值'].isin([8, 10]))),
            ((pd.to_datetime(df_xz_concat['now_date']) <= pd.to_datetime(df_xz_concat['应付日期']))),
            ((pd.notna(df_xz_concat['实付日期new'])) & (
                        pd.to_datetime(df_xz_concat['实付日期new']) <= pd.to_datetime(df_xz_concat['应付日期']))),
            ((pd.notna(df_xz_concat['实付日期new'])) & (
                        pd.to_datetime(df_xz_concat['实付日期new']) > pd.to_datetime(df_xz_concat['应付日期']))),
            ((pd.isna(df_xz_concat['实付日期new'])) & (
                        pd.to_datetime(df_xz_concat['now_date']) > pd.to_datetime(df_xz_concat['应付日期'])))]
        choices = ['已取消', '未到还款日', '正常还款', '已逾期支付', '已逾期']
        # 使用numpy.where来应用条件
        df_xz_concat['状态'] = np.select(conditions, choices, default='其他')

        # 重新定义逾期天数
        empty_rows = df_xz_concat[df_xz_concat['应付日期'].isna()]
        # 异常值删除，应付时间为空
        df_xz_concat = df_xz_concat[~df_xz_concat['应付日期'].isnull()]
        df_xz_concat['逾期天数'] = df_xz_concat.apply(lambda x:0 if x['状态'] in ['未到还款日', '正常还款', '已取消']
                                                    else (pd.to_datetime(x['实付日期new']) - pd.to_datetime(
                                                        x['应付日期'])).days if pd.to_datetime(
                                                        x['实付日期new']) >= pd.to_datetime(x['应付日期'])
                                                    else (pd.to_datetime(x['now_date']) - pd.to_datetime(
                                                        x['应付日期'])).days if pd.to_datetime(
                                                        x['now_date']) > pd.to_datetime(x['应付日期']) and pd.isna(
                                                        x['实付日期new'])
                                                    else 0, axis=1)
        # 确认是否到达表现期
        df_xz_concat['now_date'] = pd.Timestamp(datetime.now().date())
        df_xz_concat['应付日期'] = pd.to_datetime(df_xz_concat['应付日期']).dt.date
        df_xz_concat['agr_1d'] = pd.to_datetime(df_xz_concat['应付日期']) + timedelta(days=1)
        df_xz_concat['agr_4d'] = pd.to_datetime(df_xz_concat['应付日期']) + timedelta(days=4)
        df_xz_concat['agr_7d'] = pd.to_datetime(df_xz_concat['应付日期']) + timedelta(days=7)
        df_xz_concat['agr_15d'] = pd.to_datetime(df_xz_concat['应付日期']) + timedelta(days=15)
        df_xz_concat['agr_30d'] = pd.to_datetime(df_xz_concat['应付日期']) + timedelta(days=30)

        df_xz_concat['date_str'] = df_xz_concat['now_date']
        df_xz_concat['agr_1d_cum'] = np.where(
            pd.to_datetime(df_xz_concat.agr_1d) <= pd.to_datetime(df_xz_concat.date_str), 1, 0)

        df_xz_concat['agr_4d_cum'] = np.where(
            pd.to_datetime(df_xz_concat.agr_4d) <= pd.to_datetime(df_xz_concat.date_str), 1, 0)
        df_xz_concat['agr_7d_cum'] = np.where(
            pd.to_datetime(df_xz_concat.agr_7d) <= pd.to_datetime(df_xz_concat.date_str), 1, 0)
        df_xz_concat['agr_15d_cum'] = np.where(
            pd.to_datetime(df_xz_concat.agr_15d) <= pd.to_datetime(df_xz_concat.date_str), 1, 0)
        df_xz_concat['agr_30d_cum'] = np.where(
            pd.to_datetime(df_xz_concat.agr_30d) <= pd.to_datetime(df_xz_concat.date_str), 1, 0)
        # 定义观察日 每月月底时间
        df_xz_concat = self.mob_date(df_xz_concat)
        df_xz_concat['date_str'] = pd.to_datetime(df_xz_concat['date_str'])
        df_xz_concat['应付日期'] = pd.to_datetime(df_xz_concat['应付日期'])
        df_xz_concat = df_xz_concat.merge(tmp, on='order_number', how='left')
        df_xz_concat['label'] = df_xz_concat['label'].fillna(0)
        df_xz_concat['逾期天数'] = np.where(df_xz_concat['label'] == 1, 0, df_xz_concat['逾期天数'])

        return df_xz_concat

    def channel_rate(self, df_xz_concat, dfzh3):
        # （1-2）前N期的最大逾期天数 的逻辑
        # 20241222 之前的口劲，下面代码是前N期的最大逾期天数
        df_xz_concat['当前期数'] = df_xz_concat['当前期数'].astype(int)
        df_yq_fq = df_xz_concat[["order_id", "当前期数", "逾期天数"]].pivot(index="order_id", columns="当前期数",
                                                                            values="逾期天数")
        df_yq_fq = df_yq_fq.reset_index()
        df_yq_fq["FPD"] = df_yq_fq[2]
        df_yq_fq.loc[:, "SPD"] = np.where(df_yq_fq[3] > df_yq_fq[2], df_yq_fq[3], df_yq_fq[2])
        df_yq_fq.loc[:, "TPD"] = np.where(df_yq_fq[4] > df_yq_fq["SPD"], df_yq_fq[4], df_yq_fq["SPD"])
        df_yq_fq.loc[:, "QPD"] = np.where(df_yq_fq[5] > df_yq_fq["TPD"], df_yq_fq[5], df_yq_fq["TPD"])

        # （2）
        df_agr_1 = df_xz_concat[["order_id", "当前期数", "agr_1d_cum"]].pivot(index="order_id", columns="当前期数",
                                                                              values="agr_1d_cum").reset_index()
        df_agr_1 = df_agr_1.copy().add_suffix('_agr1')

        df_agr_4 = df_xz_concat[["order_id", "当前期数", "agr_4d_cum"]].pivot(index="order_id", columns="当前期数",
                                                                              values="agr_4d_cum").reset_index()
        df_agr_4 = df_agr_4.copy().add_suffix('_agr4')
        df_agr_7 = df_xz_concat[["order_id", "当前期数", "agr_7d_cum"]].pivot(index="order_id", columns="当前期数",
                                                                              values="agr_7d_cum").reset_index()
        df_agr_7 = df_agr_7.copy().add_suffix('_agr7')
        df_agr_15 = df_xz_concat[["order_id", "当前期数", "agr_15d_cum"]].pivot(index="order_id", columns="当前期数",
                                                                                values="agr_15d_cum").reset_index()
        df_agr_15 = df_agr_15.copy().add_suffix('_agr15')
        df_agr_30 = df_xz_concat[["order_id", "当前期数", "agr_30d_cum"]].pivot(index="order_id", columns="当前期数",
                                                                                values="agr_30d_cum").reset_index()
        df_agr_30 = df_agr_30.copy().add_suffix('_agr30')

        df_yq_total = pd.concat([df_yq_fq, df_agr_1, df_agr_4, df_agr_7, df_agr_15, df_agr_30], axis=1)
        # 合并数据
        df = pd.merge(dfzh3, df_yq_total, on="order_id", how="left")
        # （3）
        df["客户标签fpd4"] = np.where(df["FPD"] > 3, "逾期", "正常")
        df["客户标签fpd1"] = np.where(df["FPD"] > 0, "逾期", "正常")
        df["客户标签fpd7"] = np.where(df["FPD"] > 6, "逾期", "正常")
        df["客户标签fpd15"] = np.where(df["FPD"] > 14, "逾期", "正常")
        df["客户标签spd4"] = np.where(df["SPD"] > 3, "逾期", "正常")
        df["客户标签spd1"] = np.where(df["SPD"] > 1, "逾期", "正常")
        df["客户标签spd15"] = np.where(df["SPD"] > 14, "逾期", "正常")
        df["客户标签tpd15"] = np.where(df["TPD"] > 14, "逾期", "正常")

        df["当前逾期天数"] = df["now_overdue_days"]
        df["最长逾期天数"] = df["max_overdue_days"]
        df["还款状态"] = df["rembursement_status"]

        def getremark(a, b, c):
            if a >= c:
                return "逾期"
            elif a < c and b == "未到首期还款日":
                return "未到首期还款日"
            else:
                return "正常"

        df["当前逾期1+"] = df.apply(lambda x: getremark(x["当前逾期天数"], x["还款状态"], 1), axis=1)
        df["当前逾期1+"].value_counts()

        df["当前逾期4+"] = df.apply(lambda x: getremark(x["当前逾期天数"], x["还款状态"], 4), axis=1)
        df["当前逾期7+"] = df.apply(lambda x: getremark(x["当前逾期天数"], x["还款状态"], 7), axis=1)
        df["当前逾期15+"] = df.apply(lambda x: getremark(x["当前逾期天数"], x["还款状态"], 15), axis=1)
        df["当前逾期31+"] = df.apply(lambda x: getremark(x["当前逾期天数"], x["还款状态"], 31), axis=1)
        df["M12"] = df.apply(lambda x: getremark(x["当前逾期天数"], x["还款状态"], 366), axis=1)

        df["历史逾期1+"] = df.apply(lambda x: getremark(x["最长逾期天数"], x["还款状态"], 1), axis=1)
        df["历史逾期7+"] = df.apply(lambda x: getremark(x["最长逾期天数"], x["还款状态"], 7), axis=1)
        df["历史逾期15+"] = df.apply(lambda x: getremark(x["最长逾期天数"], x["还款状态"], 15), axis=1)
        df["历史逾期31+"] = df.apply(lambda x: getremark(x["最长逾期天数"], x["还款状态"], 31), axis=1)

        return df

    def sum_data(self, dfzh3_tmp, key, name=None, conditions=None):
        # 截止到当前前一天的日期数据
        # if int(datetime.now().strftime('%d'))>=5:
        #     month_day = (datetime.now()-pd.DateOffset(months=1)).replace(day=1).strftime('%Y-%m-%d')
        #     month_now_day = (datetime.now()-pd.DateOffset(months=1)-timedelta(days=1)).strftime('%Y-%m-%d')
        #     dfzh3_tmp = dfzh3_tmp[(dfzh3_tmp.下单日期>=month_day)&(dfzh3_tmp.下单日期<=month_now_day)]
        # else:
        #     month_day = (datetime.now() - pd.DateOffset(months=2)).replace(day=1).strftime('%Y-%m-%d')
        #     month_now_day = (datetime.now() - pd.DateOffset(months=1) - timedelta(days=1)).strftime('%Y-%m-%d')
        #     dfzh3_tmp = dfzh3_tmp[(dfzh3_tmp.下单日期 >= month_day) & (dfzh3_tmp.下单日期 <= month_now_day)]
        # conditions：判断是否是总体，从而选择使用哪个条件
        if conditions is None:
            cpd_all = dfzh3_tmp.groupby(['search_time']).agg(
                {'order_number': 'count', f'{key}_1': 'sum', f'{key}_7': 'sum', f'{key}_4': 'sum'}).reset_index()
        else:
            cpd_all = dfzh3_tmp[conditions].groupby(['search_time']).agg(
                {'order_number': 'count', f'{key}_1': 'sum', f'{key}_7': 'sum', f'{key}_4': 'sum'}).reset_index().rename(
            columns={'order_number': f'order_number_{name}', f'{key}_1': f'{key}_1_{name}', f'{key}_7': f'{key}_7_{name}', f'{key}_4': f'{key}_4_{name}'})
        return cpd_all

    def merge_data(self, dfzh3_tmp, conditions_all_2, conditions_zm, conditions_ss, conditions_drhh, conditions_sl, conditions_dy, conditions_jl, conditions_jd, key):
        cpd_all = self.sum_data(dfzh3_tmp, key)
        cpd_all_o = self.sum_data(dfzh3_tmp, key, 'o', conditions_all_2)
        cpd_all_租物渠道 = self.sum_data(dfzh3_tmp, key, '租物渠道', conditions_zm)
        cpd_all_搜索渠道 = self.sum_data(dfzh3_tmp, key, '搜索渠道', conditions_ss)
        cpd_all_单人会话 = self.sum_data(dfzh3_tmp, key, '单人会话', conditions_drhh)
        cpd_all_S量 = self.sum_data(dfzh3_tmp, key, 'S量', conditions_sl)
        cpd_all_抖音渠道 = self.sum_data(dfzh3_tmp, key, '抖音渠道', conditions_dy)
        cpd_all_拒量 = self.sum_data(dfzh3_tmp, key, '拒量', conditions_jl)
        cpd_all_京东渠道 = self.sum_data(dfzh3_tmp, key, '京东渠道', conditions_jd)
        # 关联数据
        cpd_all = cpd_all.merge(cpd_all_o, on='search_time', how='left')
        cpd_all = cpd_all.merge(cpd_all_租物渠道, on='search_time', how='left')
        cpd_all = cpd_all.merge(cpd_all_搜索渠道, on='search_time', how='left')
        cpd_all = cpd_all.merge(cpd_all_单人会话, on='search_time', how='left')
        cpd_all = cpd_all.merge(cpd_all_S量, on='search_time', how='left')
        cpd_all = cpd_all.merge(cpd_all_抖音渠道, on='search_time', how='left')
        cpd_all = cpd_all.merge(cpd_all_拒量, on='search_time', how='left')
        cpd_all = cpd_all.merge(cpd_all_京东渠道, on='search_time', how='left')
        return cpd_all
    def fpd_cpd(self, dfzh3):
        # 加工
        dfzh3_tmp = dfzh3
        dfzh3_tmp['是否拒量'] = dfzh3_tmp['是否拒量'].fillna(0)
        dfzh3_tmp['是否号卡'] = dfzh3_tmp['是否号卡'].fillna(0)
        dfzh3_tmp['now_overdue_days'] = np.where(dfzh3_tmp.是否号卡 == 1, 0, dfzh3_tmp.now_overdue_days)
        # dfzh3_tmp['max_overdue_days'] = np.where(dfzh3_tmp.是否号卡 == 1, 0, dfzh3_tmp.max_overdue_days)

        dfzh3_tmp['cpd_1'] = np.where((dfzh3_tmp['now_overdue_days'] >= 1) & (dfzh3_tmp['2_agr1'] == 1), 1, 0)
        dfzh3_tmp['cpd_4'] = np.where((dfzh3_tmp['now_overdue_days'] >= 4) & (dfzh3_tmp['2_agr4'] == 1), 1, 0)
        dfzh3_tmp['cpd_7'] = np.where((dfzh3_tmp['now_overdue_days'] >= 7) & (dfzh3_tmp['2_agr7'] == 1), 1, 0)

        dfzh3_tmp["fpd_1"] = np.where((dfzh3_tmp["FPD"] >= 1) & (dfzh3_tmp['2_agr1'] == 1), 1, 0)
        dfzh3_tmp["fpd_4"] = np.where((dfzh3_tmp["FPD"] >= 4) & (dfzh3_tmp['2_agr4'] == 1), 1, 0)
        dfzh3_tmp["fpd_7"] = np.where((dfzh3_tmp["FPD"] >= 7) & (dfzh3_tmp['2_agr7'] == 1), 1, 0)

        dfzh3_tmp["agr_1"] = np.where((dfzh3_tmp['2_agr1'] == 1), 1, 0)
        dfzh3_tmp["agr_4"] = np.where((dfzh3_tmp['2_agr4'] == 1), 1, 0)
        dfzh3_tmp["agr_7"] = np.where((dfzh3_tmp['2_agr7'] == 1), 1, 0)

        # cpd
        # 总体 总体非抖音非拒量 租物渠道 搜索渠道 抖音渠道 二手非抖音 单人聊天会话中的小程序消息卡片（分享） 拒量 S量
        conditions_all_2 = ((dfzh3_tmp.归属渠道 != '抖音渠道') & (dfzh3_tmp.是否拒量 != 1) & (dfzh3_tmp.是否号卡 != 1))
        conditions_zm = ((dfzh3_tmp.归属渠道 == '芝麻租物') & (dfzh3_tmp.是否拒量 != 1) & (dfzh3_tmp.是否号卡 != 1))
        conditions_ss = ((dfzh3_tmp.归属渠道 == '搜索渠道') & (dfzh3_tmp.是否拒量 != 1) & (dfzh3_tmp.是否号卡 != 1))
        conditions_drhh = (
                    (dfzh3_tmp.归属渠道 == '单人聊天会话中的小程序消息卡片（分享）') & (dfzh3_tmp.是否拒量 != 1) & (
                        dfzh3_tmp.是否号卡 != 1))
        conditions_sl = ((dfzh3_tmp.归属渠道 == 'S量') & (dfzh3_tmp.是否拒量 != 1) & (dfzh3_tmp.是否号卡 != 1))
        conditions_dy = (dfzh3_tmp.归属渠道 == '抖音渠道')
        conditions_jd = (dfzh3_tmp.归属渠道 == '京东渠道')
        conditions_esfdy = ((dfzh3_tmp["是否二手"] == "二手") & (dfzh3_tmp.归属渠道 != '抖音渠道') & (
                    dfzh3_tmp.是否拒量 != 1) & (dfzh3_tmp.是否号卡 != 1))
        conditions_jl = (dfzh3_tmp.是否拒量 == 1)
        # cpd
        cpd_all = self.merge_data(dfzh3_tmp, conditions_all_2, conditions_zm, conditions_ss, conditions_drhh, conditions_sl, conditions_dy, conditions_jl, conditions_jd, 'cpd')

        # fpd
        fpd_all = self.merge_data(dfzh3_tmp, conditions_all_2, conditions_zm, conditions_ss, conditions_drhh, conditions_sl, conditions_dy, conditions_jl, conditions_jd, 'fpd')

        # agr
        agr_all = self.merge_data(dfzh3_tmp, conditions_all_2, conditions_zm, conditions_ss, conditions_drhh, conditions_sl, conditions_dy, conditions_jl, conditions_jd, 'agr')

        return cpd_all, fpd_all, agr_all

    def get_rate(self, cpd_all, fpd_all, agr_all):
        # 设置索引
        cpd_all = cpd_all.set_index('search_time')
        fpd_all = fpd_all.set_index('search_time')
        agr_all = agr_all.set_index('search_time')
        cpd_rate = pd.DataFrame()
        fpd_rate = pd.DataFrame()
        # 取到列名的列表
        cpd_all_list = cpd_all.columns.tolist()
        fpd_all_list = fpd_all.columns.tolist()
        agr_all_list = agr_all.columns.tolist()
        # 循环列表获取对应的比例
        for idx, agr in enumerate(agr_all_list):
            cpd_rate.loc[:, f'{cpd_all_list[idx]}'] = cpd_all[cpd_all_list[idx]]/agr_all[agr]
            cpd_rate.loc[:, f'{cpd_all_list[idx]}'] = cpd_rate.fillna(0)[cpd_all_list[idx]].map(lambda x: format(x, '.2%'))
            fpd_rate.loc[:, f'{fpd_all_list[idx]}'] = fpd_all[fpd_all_list[idx]]/agr_all[agr]
            fpd_rate.loc[:, f'{fpd_all_list[idx]}'] = fpd_rate.fillna(0)[fpd_all_list[idx]].map(lambda x: format(x, '.2%'))
        return cpd_rate, fpd_rate

    def sort_2(self, df, df2):
        # 关联需要的数据
        df = df.merge(df2[['下单日期', 'order_id', '归属渠道', '订单状态值', '是否拒量']], on='order_id', how='left')
        # 获取特定范围内的数据和进行数据去重
        df = df[(df.当前期数==2)&(df.订单状态值==4)]
        df = df.drop_duplicates(subset=['order_id'])
        today = datetime.now().strftime('%Y-%m-%d')
        # 设置列名
        col_list = ['总体','芝麻租物','搜索渠道','单人聊天会话中的小程序消息卡片（分享）','S量','抖音渠道','拒量', '京东渠道']
        df_new = pd.DataFrame()
        # 循环列名获取对应列名的数据和使用对应的判断条件
        for col in col_list:
            if col=='总体':
                df_2 = df[(df.应付日期==today)]
            elif col=='拒量':
                df_2 = df[(df.应付日期 == today) & (df.是否拒量==1)]
            else:
                df_2 = df[(df.应付日期 == today) & (df.归属渠道==col) & (df.是否拒量==0)]
            # 计算比例

            df_2.loc[:, f'当天未还款_{col}'] = np.where(df_2.实付日期.isna(), 1, 0)
            df_2_g = df_2.groupby('下单日期').agg({'order_id': 'count', f'当天未还款_{col}': 'sum'}).rename(columns={'order_id': f'当天应付订单数_{col}'})
            df_2_g.loc[:, f'当天未付比例_{col}'] = (df_2_g[f'当天未还款_{col}']/df_2_g[f'当天应付订单数_{col}']).map(lambda x: format(x, '.2%'))
            df_new = pd.concat([df_new, df_2_g], axis=1)
        df_new = df_new.rename(columns={'当天未付比例_单人聊天会话中的小程序消息卡片（分享）':'当天未付比例_单人会话', '当天应付订单数_单人聊天会话中的小程序消息卡片（分享）': '当天应付订单数_单人会话', '当天未还款_单人聊天会话中的小程序消息卡片（分享）': '当天未还款_单人会话'})

        return df_new

    def run(self):
        print('正在查询数据...')
        df_zhys, df_out, df_cancel, df_xzfq, tmp = self.select_data()
        print('数据查询完毕！\n正在清理数据...')
        df_xz_concat, dfzh3 = self.clean_date(df_zhys, df_out, df_cancel, df_xzfq)
        print('数据清理完毕！\n正在修正日期...')
        df_xz_concat = self.update_date(df_xz_concat, tmp)
        print('日期修正完毕！\n正在计算逾期天数...')
        df = self.channel_rate(df_xz_concat, dfzh3)
        print('逾期天数计算完毕！\n正在获取fpd和cpd的数据...')
        cpd_all, fpd_all, agr_all = self.fpd_cpd(df)
        with pd.ExcelWriter('F:/rate.xlsx', engine='xlsxwriter') as writer:
            cpd_all.to_excel(writer, sheet_name='cpd_rate')
            fpd_all.to_excel(writer, sheet_name='fpd_rate')
        print('数据获取完毕！\n正在计算比例...')
        cpd_rate, fpd_rate = self.get_rate(cpd_all, fpd_all, agr_all)
        print('比例计算完毕！\n正在统计当天未还款订单...')
        df_new = self.sort_2(df_xzfq, df_xz_concat)
        print('当天未还款订单统计完毕！')
        return cpd_rate, fpd_rate, df_new

    # 构建发送信息模版
    def get_message(self, df, month=None, key=None):
        # key：判断使用哪个模板
        # month：下单月份
        if key is None:
            # 构造首逾的信息模板
            today = (datetime.now() -pd.DateOffset(months=1) - pd.Timedelta(days=3)).strftime('%Y-%m-%d')
            df = df.reset_index()
            df.loc[:, '下单日期'] = today
            df.set_index('下单日期', inplace=True)
            message = f'''
            当天应付订单数_总体：{df.loc[today, '当天应付订单数_总体']}，当天未还款_总体：{df.loc[today, '当天未还款_总体']}，当天未付比例_总体：{df.loc[today, '当天未付比例_总体']}；
            当天应付订单数_芝麻租物：{df.loc[today, '当天应付订单数_芝麻租物']}，当天未还款_芝麻租物：{df.loc[today, '当天未还款_芝麻租物']}，当天未付比例_芝麻租物：{df.loc[today, '当天未付比例_芝麻租物']}；
            当天应付订单数_搜索渠道：{df.loc[today, '当天应付订单数_搜索渠道']}，当天未还款_搜索渠道：{df.loc[today, '当天未还款_搜索渠道']}，当天未付比例_搜索渠道：{df.loc[today, '当天未付比例_搜索渠道']}；
            当天应付订单数_单人会话：{df.loc[today, '当天应付订单数_单人会话']}，当天未还款_单人会话：{df.loc[today, '当天未还款_单人会话']}，当天未付比例_单人会话：{df.loc[today, '当天未付比例_单人会话']}；
            当天应付订单数_S量：{df.loc[today, '当天应付订单数_S量']}，当天未还款_S量：{df.loc[today, '当天未还款_S量']}，当天未付比例_S量：{df.loc[today, '当天未付比例_S量']}；
            当天应付订单数_抖音渠道：{df.loc[today, '当天应付订单数_抖音渠道']}，当天未还款_抖音渠道：{df.loc[today, '当天未还款_抖音渠道']}，当天未付比例_抖音渠道：{df.loc[today, '当天未付比例_抖音渠道']}；
            当天应付订单数_拒量：{df.loc[today, '当天应付订单数_拒量']}，当天未还款_拒量：{df.loc[today, '当天未还款_拒量']}，当天未付比例_拒量：{df.loc[today, '当天未付比例_拒量']}；
            当天应付订单数_京东渠道：{df.loc[today, '当天应付订单数_京东渠道']}，当天未还款_京东渠道：{df.loc[today, '当天未还款_京东渠道']}，当天未付比例_京东渠道：{df.loc[today, '当天未付比例_京东渠道']}；
            '''
        else:
            # 构造fpd、cpd的消息模板
            df = df[df.index==month]
            message = f'''
            {key}_1+：总体：{df.loc[month, f'{key}_1']}，非抖音非拒量非号卡：{df.loc[month, f'{key}_1_o']}，芝麻租物：{df.loc[month, f'{key}_1_租物渠道']}， 搜索渠道：{df.loc[month, f'{key}_1_搜索渠道']}，单人会话：{df.loc[month, f'{key}_1_单人会话']}，S量：{df.loc[month, f'{key}_1_S量']}，抖音渠道：{df.loc[month, f'{key}_1_抖音渠道']}，拒量：{df.loc[month, f'{key}_1_拒量']}，京东渠道：{df.loc[month, f'{key}_1_京东渠道']}；
            {key}_4+：总体：{df.loc[month, f'{key}_4']}，非抖音非拒量非号卡：{df.loc[month, f'{key}_4_o']}，芝麻租物：{df.loc[month, f'{key}_4_租物渠道']}， 搜索渠道：{df.loc[month, f'{key}_4_搜索渠道']}，单人会话：{df.loc[month, f'{key}_4_单人会话']}，S量：{df.loc[month, f'{key}_4_S量']}，抖音渠道：{df.loc[month, f'{key}_4_抖音渠道']}，拒量：{df.loc[month, f'{key}_4_拒量']}，京东渠道：{df.loc[month, f'{key}_4_京东渠道']}；
            {key}_7+：总体：{df.loc[month, f'{key}_7']}，非抖音非拒量非号卡：{df.loc[month, f'{key}_7_o']}，芝麻租物：{df.loc[month, f'{key}_7_租物渠道']}， 搜索渠道：{df.loc[month, f'{key}_7_搜索渠道']}，单人会话：{df.loc[month, f'{key}_7_单人会话']}，S量：{df.loc[month, f'{key}_7_S量']}，抖音渠道：{df.loc[month, f'{key}_7_抖音渠道']}，拒量：{df.loc[month, f'{key}_7_拒量']}，京东渠道：{df.loc[month, f'{key}_7_京东渠道']}；
            '''
        return message

    # 设置钉钉机器人发送消息
    def send_dingtalk_message(self, webhook, secret, message):
        # 计算签名（如果有设置）
        if secret:
            timestamp = str(round(time.time() * 1000))
            string_to_sign = '{}\n{}'.format(timestamp, secret)
            hmac_code = hmac.new(secret.encode('utf-8'), string_to_sign.encode('utf-8'), digestmod=hashlib.sha256).digest()
            sign = base64.b64encode(hmac_code).decode('utf-8')
            webhook = f'{webhook}&timestamp={timestamp}&sign={sign}'
        # 构造消息体
        data = {
            "msgtype": "text",
            "text": {
                "content": message
            }
        }
        # 发送请求
        try:
            response = requests.post(webhook, json=data)
            response.raise_for_status()
            print("消息发送成功")
        except requests.exceptions.RequestException as e:
            print(f"消息发送失败: {e}")

    def my_job(self, hour, minute):
        print(f'执行定时任务：现在是每日的{hour}:{minute}')
        date_now = datetime.now()
        if int(date_now.strftime('%d'))>=5:
            month = (date_now-pd.DateOffset(months=1)).strftime('%Y-%m')
        else:
            month = (date_now-pd.DateOffset(months=2)).strftime('%Y-%m')
        cpd_rate, fpd_rate, df_new = self.run()
        # 获取fpd、cpd和渠道的信息
        message_fpd = self.get_message(fpd_rate, month, 'fpd')
        message_cpd = self.get_message(cpd_rate, month, 'cpd')
        message_kk = self.get_message(df_new)
        message = message_fpd+'\n'+message_cpd+'\n'+message_kk
        # self.send_dingtalk_message(self.webhook, self.secret, message)

if __name__ == '__main__':
    ri = Risk_Indicator()
    hour = 9
    minute = 15
    # ri.my_job(hour, minute)
    scheduler = BackgroundScheduler()
    job = scheduler.add_job(ri.my_job, 'cron', hour=hour, minute=minute, args=[hour, minute])
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