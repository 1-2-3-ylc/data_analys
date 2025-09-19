import pandas as pd
import numpy as np
import xlwings as xw
from datetime import datetime, timedelta
from math import log
import json
import hashlib
import requests
import time
import hmac
import hashlib
import base64
import pymysql

# 日报类
class All_Model:

    # 小蚂蚁租机
    def xmy(self, df, df_risk_examine, model):
        '''
        计算小蚂蚁租机的总体转移，机审强拒，人审拒绝，出库前风控强拒数据
        :param df: 总体数据的df
        :param df_risk_examine: 出库前风控强拒数据的df
        :param model: 传入的分组参数
        :return: 返回小蚂蚁租机的数据
        '''
        # 匹配小蚂蚁（拒绝）数据
        df_xmy = df[df['merchant_name'].isin(['小蚂蚁租机', '兴鑫兴通讯', '人人享租', '崇胜数码', '喜卓灵租机', '喜卓灵新租机'])]
        # 出库前强拒数据重命名
        df_risk_examine.rename(columns={'time': 'time_risk_ex', 'status': 'status_risk_ex'}, inplace=True)
        # 对小蚂蚁数据和出库前强拒数据进行拼接
        df_risk_examine_all = pd.merge(df_xmy, df_risk_examine, left_on='id_card_num', right_on='id_card', how='inner')
        # 计算出库前强拒的订单数
        df_risk_examine_all2 = df_risk_examine_all[(df_risk_examine_all['time_risk_ex'] < df_risk_examine_all['update_time'])]
        # 进行排序并取到最近的一个订单
        df_risk_examine_all2 = df_risk_examine_all2.sort_values(['order_id', 'time_risk_ex'], ascending=[True, False]).groupby('order_id').head(1)
        df_risk_examine_all2 = df_risk_examine_all2[df_risk_examine_all2['status_risk_ex'] == '1']
        df_risk_examine_all2_g = df_risk_examine_all2.groupby(model).agg(order_risk_ex=('order_id', 'size'))
        # df_risk_examine_all2_g
        # 排除出库前风控强拒的订单
        df_xmy_new = df_xmy[~df_xmy['order_id'].isin(df_risk_examine_all2['order_id'].to_list())]
        # 定义机审强拒和人审拒绝
        df_xmy_new['小蚂蚁机审强拒'] = np.where(df_xmy_new['reason'] == '系统风控拒绝转移', 1, 0)
        df_xmy_new['小蚂蚁人审拒绝'] = np.where(df_xmy_new['reason'] != '系统风控拒绝转移', 1, 0)
        # 计算机审强拒和人审拒绝的订单数
        df_xmy_new_g = df_xmy_new.groupby(model).agg({'order_id': 'nunique', '小蚂蚁机审强拒': 'sum', '小蚂蚁人审拒绝': 'sum'})
        df_xmy_new_g = pd.merge(df_xmy_new_g, df_risk_examine_all2_g, on=model, how='left')
        df_xmy_new_g = df_xmy_new_g.fillna(0)
        df_xmy_new_g['order_id'] = df_xmy_new_g['order_id'] + df_xmy_new_g['order_risk_ex']
        df_xmy_new_g.rename(columns={'order_id': '总订单数'}, inplace=True)
        return df_xmy_new, df_xmy_new_g
        # return df_xmy_new


    # 分组数据
    def data_group(self, df, df2, df_risk_examine, model):
        '''
        计算df按model分组的数据
        :param df: 包含小蚂蚁租机的df
        :param df2: 不含小蚂蚁租机的df
        :param df_risk_examine: 包含出库前风控强拒的df
        :param model: 需要分组的类型
        :return: 返回分组后的数据
        '''
        # 包含拒量的数据
        df_group_1 = df.groupby(model).agg({'order_id': 'size', '是否进件': 'sum', '前置拦截': 'sum'})  # , '机审通过件': 'sum', '风控通过件': 'sum'
        df_group_1.rename(columns={'order_id': '去重订单数', '是否进件': '进件数'}, inplace=True)
        # 策略241205,策略241212,自有模型回捞策略:2025.8.28：联合拒量订单
        df_241205 = df[df.tips.str.contains(r'策略241205|策略241212|命中自有模型回捞策略|回捞策略250330命中|联合拒量订单|支付宝联合运营订单', regex=True)==True]
        # 剔除支付宝联合运营订单中拒绝理由为空的记录
        df_241205 = df_241205[~((df_241205.tips.str.contains('支付宝联合运营订单')) & (df_241205['拒绝理由'].str.strip() != ''))]
        # 拒绝
        df_241205 = df_241205[~df_241205.merchant_name.isin(['小蚂蚁租机', '兴鑫兴通讯', '人人享租', '崇胜数码', '喜卓灵租机', '喜卓灵新租机'])]
        # 过往机审拒量逻辑
        # df_241205.loc[:, '机审强拒_拒量'] = np.where(df_241205.qvt_risk=='1', 1, 0)
        # 跑4月以来的数据可以这样做，跑历史数据还是需要使用旧逻辑qvt_risk=='1'
        df_241205.loc[:, '机审强拒_拒量'] = np.where(df_241205.order_id.notna(), 1, 0)
        df_241205.loc[:, '出库前强拒_拒量'] = np.where(((df_241205.qvt_risk == '0')|(df_241205.qvt_risk.isna()))&(df_241205.qvt_result == '1'), 1, 0)
        df_241205_group = df_241205.groupby(model).agg({'机审强拒_拒量': 'sum', '出库前强拒_拒量': 'sum', '是否出库': 'sum'})
        df_241205_group.rename(columns={'是否出库': '拒量出库'}, inplace=True)

        # 不包含拒量数据
        df_group_2 = df2.groupby(model).agg({'机审强拒': 'sum', '机审通过件': 'sum', '进件前取消': 'sum', '风控通过件': 'sum'
                , '人审拒绝': 'sum', '客户取消': 'sum', '已退款': 'sum', '无法联系': 'sum', '出库前风控强拒': 'sum', '待审核': 'sum', '是否出库': 'sum'}) # '已退款': 'sum',
        df_group_2.rename(columns={'是否出库': '出库'}, inplace=True)

        df_group_3 = pd.merge(df_group_1, df_group_2, on=model, how='inner')
        # 拼接下拒量的被拒数据
        df_xmy_all, df_xmy = self.xmy(df, df_risk_examine, model)
        df_group = df_group_3.merge(df_xmy, on=model, how='left').merge(df_241205_group, on=model, how='left')#.merge(df_241212_group, on=model, how='left')
        df_group = df_group.fillna(0)
        df_group['机审强拒'] = df_group['机审强拒'] + df_group['小蚂蚁机审强拒'] + df_group['机审强拒_拒量'] #+ df_group['机审通过件_241212']
        # 总体机审通过件，包含拒量
        df_group['机审通过件'] = df_group['机审通过件'] + df_group['小蚂蚁人审拒绝'] + df_group['order_risk_ex'] + df_group['出库前强拒_拒量']  # +除机审强拒的单
        df_group['风控通过件'] = df_group['风控通过件'] + df_group['order_risk_ex'] + df_group['出库前强拒_拒量']  # +出库前强拒的单
        df_group['人审拒绝'] = df_group['人审拒绝'] + df_group['小蚂蚁人审拒绝']
        df_group['出库前风控强拒'] = df_group['出库前风控强拒'] + df_group['order_risk_ex'] + df_group['出库前强拒_拒量']
        df_group['客户取消'] = df_group['客户取消']+df_group['已退款']

        df_group["拦截率"] = df_group["前置拦截"] / df_group["去重订单数"]
        df_group["拦截率"] = df_group["拦截率"].apply(lambda x: format(x, ".2%"))

        df_group["进件前取消率"] = df_group["进件前取消"] / df_group["去重订单数"]
        df_group["进件前取消率"] = df_group["进件前取消率"].apply(lambda x: format(x, ".2%"))

        df_group['预授权通过率'] = df_group["进件数"] / df_group["去重订单数"]
        df_group["预授权通过率"] = df_group["预授权通过率"].apply(lambda x: format(x, ".2%"))

        df_group["强拒比例"] = df_group["机审强拒"] / df_group["进件数"]
        df_group["强拒比例"] = df_group["强拒比例"].apply(lambda x: format(x, ".2%"))

        df_group["风控通过率"] = df_group["风控通过件"] / df_group["进件数"]
        df_group["风控通过率"] = df_group["风控通过率"].apply(lambda x: format(x, ".2%"))

        df_group["取消率"] = df_group["客户取消"] / df_group["进件数"]
        df_group["取消率"] = df_group["取消率"].apply(lambda x: format(x, ".2%"))

        df_group["无法联系占比"] = df_group["无法联系"] / df_group["进件数"]
        df_group["无法联系占比"] = df_group["无法联系占比"].apply(lambda x: format(x, ".2%"))

        df_group["人审拒绝率"] = df_group["人审拒绝"] / df_group["进件数"]
        df_group["人审拒绝率"] = df_group["人审拒绝率"].apply(lambda x: format(x, ".2%"))

        df_group["出库前强拒比例"] = df_group["出库前风控强拒"] / df_group["进件数"]
        df_group["出库前强拒比例"] = df_group["出库前强拒比例"].apply(lambda x: format(x, ".2%"))

        df_group["进件出库率"] = df_group["出库"] / df_group["进件数"]
        df_group['总体进件出库率（含拒量）'] = (df_group['出库'] + df_group['拒量出库'])/df_group["进件数"]
        df_group['拒量进件出库率增加'] = df_group['总体进件出库率（含拒量）'] - df_group['进件出库率']
        df_group["进件出库率"] = df_group["进件出库率"].apply(lambda x: format(x, ".2%"))
        df_group["总体进件出库率（含拒量）"] = df_group["总体进件出库率（含拒量）"].apply(lambda x: format(x, ".2%"))
        df_group["拒量进件出库率增加"] = df_group["拒量进件出库率增加"].apply(lambda x: format(x, ".2%"))

        df_group["订单出库率"] = df_group["出库"] / df_group["去重订单数"]
        df_group["订单出库率"] = df_group["订单出库率"].apply(lambda x: format(x, ".2%"))

        df_group["人审转化率"] = df_group["出库"] / df_group["机审通过件"]
        df_group["人审转化率"] = df_group["人审转化率"].apply(lambda x: format(x, ".2%"))

        return df_group
    
    def data_group_contain_hl(self, df, df2, df_risk_examine, model):
        '''
        计算df按model分组的数据
        :param df: 包含小蚂蚁租机的df
        :param df2: 不含小蚂蚁租机的df
        :param df_risk_examine: 包含出库前风控强拒的df
        :param model: 需要分组的类型
        :return: 返回分组后的数据
        '''
        
        # 包含拒量的数据
        df_group_1 = df.groupby(model).agg({'order_id': 'size', '是否进件': 'sum', '前置拦截': 'sum','是否出库': 'sum'})  # , '机审通过件': 'sum', '风控通过件': 'sum'
        df_group_1.rename(columns={'order_id': '去重订单数', '是否进件': '进件数', '是否出库': '出库'}, inplace=True)
        # 策略241205,策略241212,自有模型回捞策略
        df_241205 = df[df.tips.str.contains(r'策略241205|策略241212|命中自有模型回捞策略|回捞策略250330命中', regex=True)==True]
        # 拒绝
        df_241205 = df_241205[~df_241205.merchant_name.isin(['小蚂蚁租机', '兴鑫兴通讯', '人人享租', '崇胜数码', '喜卓灵租机', '喜卓灵新租机'])]
        df_241205.loc[:, '机审强拒_拒量'] = np.where(df_241205.qvt_risk=='1', 1, 0)
        df_241205.loc[:, '出库前强拒_拒量'] = np.where(((df_241205.qvt_risk == '0')|(df_241205.qvt_risk.isna()))&(df_241205.qvt_result == '1'), 1, 0)
        df_241205_group = df_241205.groupby(model).agg({'机审强拒_拒量': 'sum', '出库前强拒_拒量': 'sum', '是否出库': 'sum'})
        df_241205_group.rename(columns={'是否出库': '拒量出库'}, inplace=True)
        
        # # 不包含小蚂蚁租机（商家拒量）的数据
        # df_group_1 = df_241205.groupby(model).agg(
        #     {'order_id': 'size', '是否进件': 'sum', '前置拦截': 'sum', '是否出库': 'sum'})  # , '机审通过件': 'sum', '风控通过件': 'sum'
        # df_group_1.rename(columns={'order_id': '去重订单数', '是否进件': '进件数', '是否出库': '出库'}, inplace=True)
        # 不包含小蚂蚁租机的数据
        df_group_2 = df2.groupby(model).agg({'机审强拒': 'sum', '机审通过件': 'sum', '进件前取消': 'sum', '风控通过件': 'sum'
                , '人审拒绝': 'sum', '客户取消': 'sum', '已退款': 'sum', '无法联系': 'sum', '出库前风控强拒': 'sum', '待审核': 'sum'}) # '已退款': 'sum',, '是否出库': 'sum'
        # df_group_2.rename(columns={'是否出库': '出库'}, inplace=True)

        df_group_3 = pd.merge(df_group_1, df_group_2, on=model, how='inner')
        # 拼接下蚂蚁租机的被拒数据
        df_xmy_all, df_xmy = self.xmy(df, df_risk_examine, model)
        df_group = df_group_3.merge(df_xmy, on=model, how='left').merge(df_241205_group, on=model, how='left')
        # df_group = pd.merge(df_xmy, df_group_3, on=model, how='right')
        df_group = df_group.fillna(0)
        df_group['机审强拒'] = df_group['机审强拒'] + df_group['小蚂蚁机审强拒'] + df_group['机审强拒_拒量'] #+ df_group['机审通过件_241212']
        df_group['机审通过件'] = df_group['机审通过件'] + df_group['小蚂蚁人审拒绝'] + df_group['order_risk_ex'] + df_group['出库前强拒_拒量']  # +除机审强拒的单
        df_group['风控通过件'] = df_group['风控通过件'] + df_group['order_risk_ex'] + df_group['出库前强拒_拒量']  # +出库前强拒的单
        df_group['人审拒绝'] = df_group['人审拒绝'] + df_group['小蚂蚁人审拒绝']
        df_group['出库前风控强拒'] = df_group['出库前风控强拒'] + df_group['order_risk_ex'] + df_group['出库前强拒_拒量']
        df_group['客户取消'] = df_group['客户取消']+df_group['已退款']
        
        # df_group = df_group.groupby('下单日期')[["去重订单数","前置拦截",'进件前取消',"进件数","机审强拒","机审通过件","人审拒绝","风控通过件","客户取消","无法联系",
        #             "出库前风控强拒","待审核",'出库']].cumsum()
        # df_group = df_group.groupby('下单日期')[["去重订单数", "前置拦截",'进件前取消', "进件数", "机审强拒", "机审通过件", "人审拒绝", "风控通过件", "客户取消",
        #                                     "无法联系","出库前风控强拒", "待审核", '出库']].last()

        df_group["拦截率"] = df_group["前置拦截"] / df_group["去重订单数"]
        df_group["拦截率"] = df_group["拦截率"].apply(lambda x: format(x, ".2%"))

        df_group["进件前取消率"] = df_group["进件前取消"] / df_group["去重订单数"]
        df_group["进件前取消率"] = df_group["进件前取消率"].apply(lambda x: format(x, ".2%"))

        df_group['预授权通过率'] = df_group["进件数"] / df_group["去重订单数"]
        df_group["预授权通过率"] = df_group["预授权通过率"].apply(lambda x: format(x, ".2%"))

        df_group["强拒比例"] = df_group["机审强拒"] / df_group["进件数"]
        df_group["强拒比例"] = df_group["强拒比例"].apply(lambda x: format(x, ".2%"))

        # df_group["机审通过件"] = df_group["进件数"]-df_group["机审强拒"]

        # df_group["风控通过件"] = df_group["进件数"]-df_group["机审强拒"]-df_group["人审拒绝"]

        df_group["风控通过率"] = df_group["风控通过件"] / df_group["进件数"]
        df_group["风控通过率"] = df_group["风控通过率"].apply(lambda x: format(x, ".2%"))

        df_group["取消率"] = df_group["客户取消"] / df_group["进件数"]
        df_group["取消率"] = df_group["取消率"].apply(lambda x: format(x, ".2%"))

        df_group["无法联系占比"] = df_group["无法联系"] / df_group["进件数"]
        df_group["无法联系占比"] = df_group["无法联系占比"].apply(lambda x: format(x, ".2%"))

        df_group["人审拒绝率"] = df_group["人审拒绝"] / df_group["进件数"]
        df_group["人审拒绝率"] = df_group["人审拒绝率"].apply(lambda x: format(x, ".2%"))

        df_group["出库前强拒比例"] = df_group["出库前风控强拒"] / df_group["进件数"]
        df_group["出库前强拒比例"] = df_group["出库前强拒比例"].apply(lambda x: format(x, ".2%"))

        df_group["进件出库率"] = df_group["出库"] / df_group["进件数"]
        df_group["进件出库率"] = df_group["进件出库率"].apply(lambda x: format(x, ".2%"))

        df_group["订单出库率"] = df_group["出库"] / df_group["去重订单数"]
        df_group["订单出库率"] = df_group["订单出库率"].apply(lambda x: format(x, ".2%"))

        df_group["人审转化率"] = df_group["出库"] / df_group["机审通过件"]
        df_group["人审转化率"] = df_group["人审转化率"].apply(lambda x: format(x, ".2%"))

        return df_group
        

    def data_group_hour(self, df, df2, df_risk_examine, model):
        '''
        计算df按model分组的数据
        :param df: 包含小蚂蚁租机的df
        :param df2: 不含小蚂蚁租机的df
        :param df_risk_examine: 包含出库前风控强拒的df
        :param model: 需要分组的类型
        :return: 返回分组后的数据
        '''
        # 包含小蚂蚁租机的数据
        df_group_1 = df.groupby(model).agg(
            {'order_id': 'size', '是否进件': 'sum', '前置拦截': 'sum'})  # , '机审通过件': 'sum', '风控通过件': 'sum'
        df_group_1.rename(columns={'order_id': '去重订单数', '是否进件': '进件数', '是否出库': '出库'}, inplace=True)
        # 不包含小蚂蚁租机的数据
        df_group_2 = df2.groupby(model).agg(
            {'机审强拒': 'sum', '机审通过件': 'sum', '进件前取消': 'sum', '风控通过件': 'sum'
                , '人审拒绝': 'sum', '客户取消': 'sum', '无法联系': 'sum', '出库前风控强拒': 'sum', '待审核': 'sum', '是否出库': 'sum'}) # '已退款': 'sum',
        df_group_2.rename(columns={'是否出库': '出库'}, inplace=True)

        df_group_3 = pd.merge(df_group_1, df_group_2, on=model, how='inner')
        # 拼接下蚂蚁租机的被拒数据
        df_xmy = self.xmy(df, df_risk_examine, model)
        df_group = pd.merge(df_xmy, df_group_3, on=model, how='right')
        df_group = df_group.fillna(0)
        df_group['机审强拒'] = df_group['机审强拒'] + df_group['小蚂蚁机审强拒']
        df_group['机审通过件'] = df_group['机审通过件'] + df_group['小蚂蚁人审拒绝'] + df_group['order_risk_ex']  # +除机审强拒的单
        df_group['风控通过件'] = df_group['风控通过件'] + df_group['order_risk_ex']  # +出库前强拒的单
        df_group['人审拒绝'] = df_group['人审拒绝'] + df_group['小蚂蚁人审拒绝']
        df_group['出库前风控强拒'] = df_group['出库前风控强拒'] + df_group['order_risk_ex']

        df_group = df_group.groupby('下单日期')[["去重订单数","前置拦截",'进件前取消',"进件数","机审强拒","机审通过件","人审拒绝","风控通过件","客户取消","无法联系",
                    "出库前风控强拒","待审核",'出库']].cumsum()
        df_group = df_group.groupby('下单日期')[["去重订单数", "前置拦截",'进件前取消', "进件数", "机审强拒", "机审通过件", "人审拒绝", "风控通过件", "客户取消",
                                            "无法联系","出库前风控强拒", "待审核", '出库']].last()

        df_group["拦截率"] = df_group["前置拦截"] / df_group["去重订单数"]
        df_group["拦截率"] = df_group["拦截率"].apply(lambda x: format(x, ".2%"))

        df_group["进件前取消率"] = df_group["进件前取消"] / df_group["去重订单数"]
        df_group["进件前取消率"] = df_group["进件前取消率"].apply(lambda x: format(x, ".2%"))

        df_group['预授权通过率'] = df_group["进件数"] / df_group["去重订单数"]
        df_group["预授权通过率"] = df_group["预授权通过率"].apply(lambda x: format(x, ".2%"))

        df_group["强拒比例"] = df_group["机审强拒"] / df_group["进件数"]
        df_group["强拒比例"] = df_group["强拒比例"].apply(lambda x: format(x, ".2%"))

        # df_group["机审通过件"] = df_group["进件数"]-df_group["机审强拒"]

        # df_group["风控通过件"] = df_group["进件数"]-df_group["机审强拒"]-df_group["人审拒绝"]

        df_group["风控通过率"] = df_group["风控通过件"] / df_group["进件数"]
        df_group["风控通过率"] = df_group["风控通过率"].apply(lambda x: format(x, ".2%"))

        df_group["取消率"] = df_group["客户取消"] / df_group["进件数"]
        df_group["取消率"] = df_group["取消率"].apply(lambda x: format(x, ".2%"))

        df_group["无法联系占比"] = df_group["无法联系"] / df_group["进件数"]
        df_group["无法联系占比"] = df_group["无法联系占比"].apply(lambda x: format(x, ".2%"))

        df_group["人审拒绝率"] = df_group["人审拒绝"] / df_group["进件数"]
        df_group["人审拒绝率"] = df_group["人审拒绝率"].apply(lambda x: format(x, ".2%"))

        df_group["出库前强拒比例"] = df_group["出库前风控强拒"] / df_group["进件数"]
        df_group["出库前强拒比例"] = df_group["出库前强拒比例"].apply(lambda x: format(x, ".2%"))

        df_group["进件出库率"] = df_group["出库"] / df_group["进件数"]
        df_group["进件出库率"] = df_group["进件出库率"].apply(lambda x: format(x, ".2%"))

        df_group["订单出库率"] = df_group["出库"] / df_group["去重订单数"]
        df_group["订单出库率"] = df_group["订单出库率"].apply(lambda x: format(x, ".2%"))

        df_group["人审转化率"] = df_group["出库"] / df_group["机审通过件"]
        df_group["人审转化率"] = df_group["人审转化率"].apply(lambda x: format(x, ".2%"))

        return df_group


    # 机型内存转化
    def product_model(self, df, df2, keys):
        if keys == 'All':
            df = df.copy()
            df2 = df2.copy()
        elif keys == '芝麻租物':
            df = df[df["归属渠道"] == "芝麻租物"]
            df2 = df2[df2["归属渠道"] == "芝麻租物"]
        elif keys == '全新':
            df = df[(df["归属渠道"] == "芝麻租物") & (df["商品类型"] == "全新")]
            df2 = df2[(df2["归属渠道"] == "芝麻租物") & (df2["商品类型"] == "全新")]
        # df["型号内存"] = df["product_name"] + "_" + df["内存"]
        # df2["型号内存"] = df2["product_name"] + "_" + df2["内存"]
        df["机型内存"] = df["机型"] + "_" + df["内存"]
        df2["机型内存"] = df2["机型"] + "_" + df2["内存"]

        df_group1 = df.groupby(['机型内存']).agg({'order_id': 'size', '是否进件': 'sum'})
        df_group2 = df2.groupby(['机型内存']).agg(
            {'前置拦截': 'sum', '机审强拒': 'sum', '人审拒绝': 'sum', '客户取消': 'sum'
                , '无法联系': 'sum', '出库前风控强拒': 'sum', '待审核': 'sum', '是否出库': 'sum'})
        df_group1.rename(columns={'order_id': '去重订单数'}, inplace=True)

        df_group = pd.merge(df_group1, df_group2, on='机型内存', how='inner')

        df_group["拦截率"] = df_group["前置拦截"] / df_group["去重订单数"]
        df_group["拦截率"] = df_group["拦截率"].apply(lambda x: format(x, ".2%"))

        df_group['预授权通过率'] = df_group["是否进件"] / df_group["去重订单数"]
        df_group["预授权通过率"] = df_group["预授权通过率"].apply(lambda x: format(x, ".2%"))

        df_group["强拒比例"] = df_group["机审强拒"] / df_group["是否进件"]
        df_group["强拒比例"] = df_group["强拒比例"].apply(lambda x: format(x, ".2%"))

        df_group["机审通过件"] = df_group["是否进件"] - df_group["机审强拒"]

        df_group["风控通过件"] = df_group["是否进件"] - df_group["机审强拒"] - df_group["人审拒绝"]
        df_group["风控通过率"] = df_group["风控通过件"] / df_group["是否进件"]
        df_group["风控通过率"] = df_group["风控通过率"].apply(lambda x: format(x, ".2%"))

        df_group["取消率"] = df_group["客户取消"] / df_group["是否进件"]
        df_group["取消率"] = df_group["取消率"].apply(lambda x: format(x, ".2%"))

        # df_group["无法联系占比"]=df_group["无法联系"]/df_group["是否进件"]
        # df_group["无法联系占比"]=df_group["无法联系占比"].apply(lambda x:format(x,".2%"))

        df_group["人审拒绝率"] = df_group["人审拒绝"] / df_group["是否进件"]
        df_group["人审拒绝率"] = df_group["人审拒绝率"].apply(lambda x: format(x, ".2%"))

        df_group["进件出库率"] = df_group["是否出库"] / df_group["是否进件"]
        df_group["进件出库率"] = df_group["进件出库率"].apply(lambda x: format(x, ".2%"))

        df_group["订单出库率"] = df_group["是否出库"] / df_group["去重订单数"]
        df_group["订单出库率"] = df_group["订单出库率"].apply(lambda x: format(x, ".2%"))

        df_group = df_group[
            ["去重订单数", "前置拦截", "拦截率", "是否进件", "预授权通过率", "机审强拒", "强拒比例", "机审通过件", "人审拒绝", "风控通过件", "风控通过率"
                , "客户取消", "无法联系", "出库前风控强拒", "待审核", "是否出库", "进件出库率", "订单出库率", "取消率", "人审拒绝率"]]

        return df_group


    # 商家转化
    def merchant_names(self,  df_contain, name,  model):
        df0 = df_contain[df_contain["merchant_name"] == name]
        df0['型号内存'] = df0['product_name'] + '_' + df_contain['内存']
        if model=='机型':
            df0['机型'] = df0['product_name'].str.replace(' ', '', regex=False).str.extract(r'(iPhone ?\d+(ProMax|Pro|Max)?)')[0]
            df_group = df0.groupby('机型').agg(
                {'order_id': 'size', '是否进件': 'sum', '进件前取消': 'sum', '前置拦截': 'sum', '机审强拒': 'sum',
                    '人审拒绝': 'sum', '客户取消': 'sum', '无法联系': 'sum', '出库前风控强拒': 'sum', '待审核': 'sum',
                    '是否出库': 'sum'})
            df_group.rename(columns={'order_id': '去重订单数', '是否进件': '进件数', '是否出库': '出库'}, inplace=True)
        else:
            df_group = df0.groupby(model).agg(
                {'order_id': 'size', '是否进件': 'sum', '进件前取消': 'sum', '前置拦截': 'sum', '机审强拒': 'sum',
                    '人审拒绝': 'sum', '客户取消': 'sum', '无法联系': 'sum', '出库前风控强拒': 'sum', '待审核': 'sum',
                    '是否出库': 'sum'})
            df_group.rename(columns={'order_id': '去重订单数', '是否进件': '进件数', '是否出库': '出库'}, inplace=True)
        
        
        
        df_group["拦截率"] = df_group["前置拦截"] / df_group["去重订单数"]
        df_group["拦截率"] = df_group["拦截率"].apply(lambda x: format(x, ".2%"))

        df_group["进件前取消率"] = df_group["进件前取消"] / df_group["去重订单数"]
        df_group["进件前取消率"] = df_group["进件前取消率"].apply(lambda x: format(x, ".2%"))

        df_group['预授权通过率'] = df_group["进件数"] / df_group["去重订单数"]
        df_group["预授权通过率"] = df_group["预授权通过率"].apply(lambda x: format(x, ".2%"))

        df_group["强拒比例"] = df_group["机审强拒"] / df_group["进件数"]
        df_group["强拒比例"] = df_group["强拒比例"].apply(lambda x: format(x, ".2%"))

        df_group["机审通过件"] = df_group["进件数"] - df_group["机审强拒"]

        df_group["风控通过件"] = df_group["进件数"] - df_group["机审强拒"] - df_group["人审拒绝"]
        df_group["风控通过率"] = df_group["风控通过件"] / df_group["进件数"]
        df_group["风控通过率"] = df_group["风控通过率"].apply(lambda x: format(x, ".2%"))

        df_group["取消率"] = df_group["客户取消"] / df_group["进件数"]
        df_group["取消率"] = df_group["取消率"].apply(lambda x: format(x, ".2%"))
        # 临时恢复无法联系
        df_group["无法联系占比"]=df_group["无法联系"]/df_group["进件数"]
        df_group["无法联系占比"]=df_group["无法联系占比"].apply(lambda x:format(x,".2%"))
        
        # 临时添加
        df_group["出库前强拒比例"] = df_group["出库前风控强拒"] / df_group["进件数"]
        df_group["出库前强拒比例"] = df_group["出库前强拒比例"].apply(lambda x: format(x, ".2%"))


        df_group["人审拒绝率"] = df_group["人审拒绝"] / df_group["进件数"]
        df_group["人审拒绝率"] = df_group["人审拒绝率"].apply(lambda x: format(x, ".2%"))

        df_group["进件出库率"] = df_group["出库"] / df_group["进件数"]
        df_group["进件出库率"] = df_group["进件出库率"].apply(lambda x: format(x, ".2%"))

        df_group["订单出库率"] = df_group["出库"] / df_group["去重订单数"]
        df_group["订单出库率"] = df_group["订单出库率"].apply(lambda x: format(x, ".2%"))

        return df_group


    # 自动粘贴数据
    def Open_Excel(self, df, path, password, sheet_name,  col_len=0, key=None):
        # 定义文件路径和密码
        file_path = path
        password = password
        # 使用 xlwings 打开加密的 Excel 文件
        app = xw.App(visible=False)
        # 获取今天是星期几
        week = pd.to_datetime(datetime.now().strftime('%Y-%m-%d')).day_name()
        try:
            wb = xw.Book(file_path, password=password)
            # 获取工作表
            sheet = wb.sheets[sheet_name]
            # 获取该工作表的最后一行，并插入一行
            last_row = sheet.used_range.last_cell.row
            # col_len:判断是否是第一列第一个需要插入的数据；
            # key：值等于1用来判断是否是免人审数据，免审订单转化，押金类型这三个数据页在最后一行的前三行插入； 值不等于1则在最后一行插入并且起始行数最后减4
            if col_len == 0:
                if key is None:
                    if week=='Monday':
                        sheet.api.Rows(f'{last_row}:{last_row + 2}').Insert()
                    else:
                        sheet.api.Rows(last_row).Insert()
                else:
                    if week=='Monday':
                        sheet.api.Rows(f'{(last_row-2)}:{last_row}').Insert()
                    else:
                        sheet.api.Rows(last_row-3).Insert()

            # 重新获取最后一行
            last_row = sheet.used_range.last_cell.row
            # 将 DataFrame 写入固定的位置
            # 对应Excel中的行和列
            if key is None:
                start_row = last_row - len(df)-2
            else:
                start_row = last_row - len(df) - 4
            start_col = col_len + 1
            # 获取上一个df的宽度并相加
            col_lens = start_col + len(df.columns)
            # 写入 DataFrame 的数据
            for row_num, (_, row_data) in enumerate(df.iterrows(), start=start_row + 1):
                for new_row, value in enumerate(row_data, start=start_col):
                    sheet.cells(row_num, new_row).value = value

            # 保存并重新加密文件
            wb.save(password=password)
            return col_lens
        finally:
            # 确保关闭工作簿和应用程序实例
            wb.close()
            app.quit()

    # 2024-04-30更新
    def Open_Excel2(self, df, path, password, sheet_name, col, col_len=0, key=None):
        '''
        对日报数据进行自动更新
        :param df: 日报各个渠道的df
        :param path: 日报的excel文件路径
        :param password: excel文件的密码
        :param sheet_name: excel的sheet页名称
        :param col: 列的位置，如A列，B列等
        :param col_len: 每个模块之间的间隔宽度
        :param key: 判断是否有进行合计
        :return:
        '''
        # 定义文件路径和密码
        file_path = path
        password = password
        # 使用 xlwings 打开加密的 Excel 文件
        app = xw.App(visible=False)
        # 获取今天是星期几
        week = pd.to_datetime(datetime.now().strftime('%Y-%m-%d')).day_name()
        try:
            wb = xw.Book(file_path, password=password)
            # 获取工作表
            sheet = wb.sheets[sheet_name]
            # 获取该工作表的最后一行，并插入一行
            # col_len:判断是否是第一列第一个需要插入的数据；
            # key：值等于1用来判断是否是免人审数据，免审订单转化，押金类型这三个数据页在最后一行的前三行插入； 值不等于1则在最后一行插入并且起始行数最后减4
            # 获取最后一行的日期数据进行判断
            last_row = sheet.range(col + str(sheet.cells.last_cell.row)).end('up').row
            if key is None:
                last_date = sheet.range(f'{col}{last_row - 2}').value
            elif key == 1:
                last_date = sheet.range(f'{col}{last_row - 3}').value
            elif key == '押金':
                last_date = sheet.range(f'{col}{last_row - 4}').value
            else:
                last_date = sheet.range(f'{col}{last_row}').value
            # 获取前一天的日期
            before_date = pd.to_datetime((datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'))
            if col_len == 0:
                if before_date != last_date:
                    diff = (before_date - last_date).days
                    if key is None or (key != 1 and key != '押金'):
                        # 判断最后一行日期是不是前一天的日期，如不是则差几天就插入几行，如是则不插入新的行
                        if diff == 1:
                            sheet.api.Rows(last_row).Insert()
                        else:
                            # 从最后一行开始插入
                            sheet.api.Rows(f'{last_row}:{last_row + diff - 1}').Insert()
                    else:
                        if diff == 1:
                            sheet.api.Rows(last_row - 3).Insert()
                        else:
                            # 从最后一行的前几行开始插入
                            sheet.api.Rows(f'{last_row - diff + 1}:{last_row}').Insert()

            # 重新获取最后一行
            last_row = sheet.used_range.last_cell.row
            # 将 DataFrame 写入固定的位置
            # 对应Excel中的行和列，起始行数
            start_row = 0
            if key is None or (key != 1 and key != '押金'):
                start_row = last_row - len(df) - 2
            elif key == 1 or key == '押金':
                start_row = last_row - len(df) - 4
            start_col = col_len + 1
            # 获取上一个df的宽度并相加
            col_lens = start_col + len(df.columns)
            # 写入 DataFrame 的数据
            for row_num, (_, row_data) in enumerate(df.iterrows(), start=start_row + 1):
                for new_row, value in enumerate(row_data, start=start_col):
                    sheet.cells(row_num, new_row).value = value

            # 保存并重新加密文件
            wb.save(password=password)
            return col_lens
        finally:
            # 确保关闭工作簿和应用程序实例
            wb.close()
            app.quit()
# all_models = All_Model()
# print(all_models.__dir__())
# 数据处理类
class Data_Clean:
    # 连接数据库
    def query(self, sql,host="rr-wz9wx0w3yti9d4f6wro.mysql.rds.aliyuncs.com",user="ylc",password="1O8t5lcJ5aMhwwPEUUjS",database='',port=3306):
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
    # 定义订单状态 更改项：xx is not np.nan -> pd.notna(xx)
    def reject_type(self, a, b, c, d, e, f, g, h, i, j):
        '''
        判断订单状态
        :param a:拒绝理由
        :param b:进件
        :param c:电审拒绝原因
        :param d:取消原因
        :param e:status2
        :param f:无法联系原因
        :param g:total_describes
        :param h:是否前置拦截
        :param i:是否机审强拒
        :param j:是否出库前风控强拒
        :return:
        '''
        if h == 1 and b == "未进件":
            return "前置拦截"
        elif i == 1 and b == "进件":
            return "机审强拒"
        elif b == "进件" and pd.notna(c) and '已退款' in e:
            return "人审拒绝"
        elif b == "进件" and pd.notna(d) and str(d).strip():
            return "客户取消"
        elif b == "进件" and pd.notna(f) and str(f).strip():
            return "无法联系"
        elif b == "进件" and j == 1:
            return "出库前风控强拒"
        elif b == "进件" and "待审核" in e:
            return "待审核"
        elif b == "进件" and "待发货" in e:
            return "出库"
        elif b == "进件" and "待收货" in e:
            return "出库"
        elif b == "进件" and "租赁中" in e:
            return "出库"
        elif b == "进件" and "已完成" in e:
            return "出库"
        elif pd.isna(a) and b == "未进件":
            return "未进件"
        elif b == "未进件":
            return "进件前取消"
        else:
            return e

    # 删除商家数据
    def drop_merchant(self, df):
        '''
        删除商家数据
        :param df: 传入带有商家的数据
        :return: 返回剔除了商家的数据
        '''
        # 剔除商家数据只保留自营租机业务数据
        df.drop(df[df['merchant_name'] == "深圳优优大数据科技有限公司"].index, inplace=True)
        df.drop(df[df['merchant_name'] == "优优2店"].index, inplace=True)
        df.drop(df[df['merchant_name'] == "小豚租（代收）"].index, inplace=True)
        df.drop(df[df['merchant_name'] == "苏州蚁诺宝"].index, inplace=True)
        df.drop(df[df['merchant_name'] == "租着用电脑数码"].index, inplace=True)
        df.drop(df[df['merchant_name'] == "北京海鸟窝科技有限公司"].index, inplace=True)

        df.drop(df[df['merchant_name'] == "汇客好租"].index, inplace=True)
        df.drop(df[df['merchant_name'] == "澄心优租"].index, inplace=True)
        df.drop(df[df['merchant_name'] == "CPS渠道合作"].index, inplace=True)
        df.drop(df[df['sku_attributes'].str.contains(pat='探路者', regex=False) == True].index, inplace=True)
        # 趣智数码  单
        df.drop(df[df['merchant_name'] == "趣智数码"].index, inplace=True)
        df.drop(df[df['merchant_name'] == "格木木二奢名品"].index, inplace=True)
        df.drop(df[df['merchant_name'] == "广州康基贸易有限公司"].index, inplace=True)
        df.drop(df[df['merchant_name'] == "线下小店"].index, inplace=True)
        df.drop(df[df['merchant_name'] == "乙辉数码"].index, inplace=True)
        df.drop(df[df['merchant_name'] == "呱子笔记本电脑"].index, inplace=True)
        df.drop(df[df['merchant_name'] == "南京聚格网络科技"].index, inplace=True)
        
        df.drop(df[df['merchant_name'] == "星晟数码"].index, inplace=True)
        df.drop(df[df['merchant_name'] == "蘑菇时间"].index, inplace=True)

        df.drop(df[df['merchant_name'].str.contains(pat='探路者', regex=False) == True].index, inplace=True)
        return df

    # 删除拒量数据
    def drop_rejected_merchant(self, df):
        # df.drop(df[df['merchant_name'] == "小蚂蚁租机"].index, inplace=True)
        # df.drop(df[df['merchant_name'] == "兴鑫兴通讯"].index, inplace=True)
        # df.drop(df[df['merchant_name'] == "人人享租"].index, inplace=True)
        # df.drop(df[df['merchant_name'] == "崇胜数码"].index, inplace=True)
        # df.drop(df[df['merchant_name'] == "喜卓灵租机"].index, inplace=True)
        # df.drop(df[df['merchant_name'] == "喜卓灵新租机"].index, inplace=True)
        # 将多行drop操作合并为一行
        reject_merchants = ["小蚂蚁租机", "兴鑫兴通讯", "人人享租", "崇胜数码", "喜卓灵租机", "喜卓灵新租机"]
        df.drop(df[df['merchant_name'].isin(reject_merchants)].index, inplace=True)
                
        if '机审通过件' in df.columns:
            df = df[~((df.机审通过件 == 1) & (df.tips.str.contains(r'策略241205') == True))]
            df = df[~((df.机审通过件==1)&(df.tips.str.contains('策略241212', regex=False)==True))]
            df = df[~((df.机审通过件==1)&(df.tips.str.contains('命中自有模型回捞策略', regex=False)==True))]
            df = df[~((df.机审通过件==1)&(df.tips.str.contains('回捞策略250330命中', regex=False)==True))]
            df = df[~((df.机审通过件==1)&(df.tips.str.contains('联合拒量订单', regex=False)==True))]
            # to_do支付宝联合运营订单不为拒量
            # df = df[~((df.机审通过件==1)&(df.tips.str.contains('支付宝联合运营订单', regex=False)==True))]
        return df

    # 订单去重
    def order_drop_duplicates(self, df):
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
        # 删除订单状态空值行
        df.dropna(subset=["status2"], axis=0, inplace=True)
        # 删除重复订单
        df.drop_duplicates(subset=["order_id"], inplace=True)
        df.drop_duplicates(subset=["true_name", "user_mobile", "id_card_num", "下单日期"], keep="last", inplace=True)
        # df.drop(df[df['true_name'].isin(
        #     [" ", "谢仕程", "潘立", "洪柳", "陈锦奇", "周杰", "卢腾标", "孔靖", "黄娟", "钟福荣", "邱锐杰", "唐林华"
        #         , "邓媛斤", "黄子南", "刘莎莎", "赖瑞彤", "孙子文", '淦文豪', '杨明豪', '闫宇龙'])].index, inplace=True)
        return df

    # 填充日期不连续的数据
    def continuous_dates(self, df, key=None):
        # 计算日期差值，找出不连续的地方
        date_diffs = df.index.to_series().diff().dt.days.fillna(1)
        # 找出不连续的点
        discontinuities = date_diffs != 1
        if df.index.max()!=datetime.now().date():
            # 创建一个完整的日期范围
            full_date_range = pd.date_range(start=df.index.min(), end=datetime.now().date(), freq='D')
        else:
            # 创建一个完整的日期范围
            full_date_range = pd.date_range(start=df.index.min(), end=df.index.max(), freq='D')
        # # 使用reindex方法添加缺失的日期
        df_complete = df.reindex(full_date_range)
        if key is None:
            df_complete.fillna(0, inplace=True)
        if len(df_complete) < 16:
            needed_dates = pd.date_range(
                start=df_complete.index.min() - pd.Timedelta(days=(16 - len(df_complete))),
                periods=16 - len(df_complete),
                freq='D'
            )
            # 创建一个新的DataFrame用于填充的数据，默认值设为0
            filler_df = pd.DataFrame(0, index=needed_dates, columns=df.columns)

            # 将填充的数据和原数据合并
            df_complete = pd.concat([filler_df, df_complete])
        return df_complete

    # 渠道归属
    def qudao_type(self, ly_channel, activity_name, order_method, channel_type_id, order_type):
        ly_channel = str(ly_channel)
        activity_name = str(activity_name)
        if (channel_type_id in [2, 3, 80] and order_type=='ZFB_ORDER')  or order_method == 1:
            return "芝麻租物"
        elif order_type=='DY_ORDER':
            return "抖音渠道"
        elif channel_type_id in [48, 81] and order_type=='ZFB_ORDER' and '灯火联投测试' not in ly_channel:
            return "搜索渠道"
        elif order_type=='JD_ORDER':
            return '京东渠道'
        elif "支付宝直播" in ly_channel or '支付宝直播' in activity_name:
            return "支付宝直播"
        elif "直播" in ly_channel:
            return "CPS直播"
        elif "繁星" in ly_channel:
            return "繁星"
        elif "生活号" in activity_name:
            return "生活号"
        elif "群" in activity_name:
            return "支付宝社群"
        else:
            return ly_channel

    # 定义状态
    def status_node(self, df):
        df["待审核"] = np.where(df["审核状态"] == '待审核', 1, 0)
        df["前置拦截"] = np.where(df["审核状态"] == '前置拦截', 1, 0)
        df["人审拒绝"] = np.where(df["审核状态"] == '人审拒绝', 1, 0)
        df["客户取消"] = np.where(df["审核状态"] == '客户取消', 1, 0)
        df["无法联系"] = np.where(df["审核状态"] == '无法联系', 1, 0)
        df["是否进件"] = np.where(df["进件"] == '进件', 1, 0)
        df["是否出库"] = np.where(df["status"].isin([2, 3, 4, 5, 6, 8, 15]), 1, 0)

        df["进件前取消"] = np.where(df["审核状态"] == '进件前取消', 1, 0)

        df['是否出库'] = np.where((df['人审拒绝'] == 0) & (df['客户取消'] == 0) & (df['无法联系'] == 0) & (df['待审核'] == 0) & (df['是否出库'] == 1), 1, 0)
        df["出库前风控强拒"] = np.where((df["审核状态"] == '出库前风控强拒') & (df['是否出库'] == 0), 1, 0)
        df["机审强拒"] = np.where((df["审核状态"] == '机审强拒') & (df['是否出库'] == 0), 1, 0)
        df['机审通过件'] = np.where((df['是否进件'] == 1) & (df['机审强拒'] == 0), 1, 0)
        df['风控通过件'] = np.where((df['是否进件'] == 1) & (df['机审强拒'] == 0) & (df['人审拒绝'] == 0), 1, 0)

        df['已退款'] = np.where((df['风控通过件'] == 1) & (df['审核状态'] == '已退款'), 1, 0)

        df['是否二手'] = np.where(df['product_name'].str.contains(r'99新|95新|准新|90新'), 1, 0)
        df['是否拒量'] = np.where(df.tips.str.contains(r'策略2412|命中自有模型回捞策略|回捞策略250330命中|联合拒量订单') == True, 1, 0)
        return df

    # 定义一个函数来计算年龄
    def get_age(self, id_card, order_time):
        if id_card[:17].isnumeric() and len(id_card) == 18:
            birth_year = int(id_card[6:10])
            birth_month = int(id_card[10:12])
            birth_day = int(id_card[12:14])
            birth_date = datetime(birth_year, birth_month, birth_day)
            age = order_time.year - birth_date.year - ((order_time.month, order_time.day) < (birth_date.month, birth_date.day))
            return age

    # 定义一个函数来判断性别
    def get_gender(self, id_card):
        gender_digit = int(id_card[-2])  # 第17位数字
        return '男' if gender_digit % 2 != 0 else '女'

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

    # MD5加密
    def md5_hash(self, text):
        if pd.isna(text):  # 检查是否为空值
            return None
        else:
            md5 = hashlib.md5()  # 创建一个md5对象
            md5.update(text.encode('utf-8'))  # 使用utf-8编码数据
            return md5.hexdigest()  # 返回加密后的十六进制字符串


    # 设置钉钉机器人发送消息
    def send_dingtalk_message(self, webhook, secret, message):
        # 计算签名（如果有设置）
        if secret:
            timestamp = str(round(time.time() * 1000))
            string_to_sign = '{}\n{}'.format(timestamp, secret)
            hmac_code = hmac.new(secret.encode('utf-8'), string_to_sign.encode('utf-8'),
                                 digestmod=hashlib.sha256).digest()
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


# 周报类
class Week_Model:
    all_models = All_Model()
    clean = Data_Clean()
    # 计算环比
    def calculate_growth_rate(self, current, previous):
        '''
        计算数据的环比
        :param current: 当前的数据
        :param previous: 前一个数据
        :return:
        '''
        if previous == 0:
            return float('inf') if current > 0 else float('-inf')
        return (current - previous) / previous


    # 定义一个自定义的时间频率
    def custom_weekly_resampler(self, df, key):
        '''
        W：默认表示每周六结束（即周五开始）。
        W-MON：表示每周一结束（即周二开始）。
        W-TUE：表示每周二结束（即周三开始）。
        W-WED：表示每周三结束（即周四开始）。
        W-THU：表示每周四结束（即周五开始）。
        W-FRI：表示每周五结束（即周六开始）。
        W-SAT：表示每周六结束（即周日开始）。
        W-SUN：表示每周日结束（即周一开始）。
        :param df: 传入的df，需要包含日期
        :return: 返回增加了按周的df
        '''
        # 初始化一个日期
        week_dates = None
        # 获取每周需要的日期
        if key == 1:
            week_dates = df.index.to_period('W-MON').to_timestamp('W-MON')
        elif key == 2:
            week_dates = df.index.to_period('W-TUE').to_timestamp('W-TUE')
        elif key == 3:
            week_dates = df.index.to_period('W-WED').to_timestamp('W-WED')
        elif key == 4:
            week_dates = df.index.to_period('W-THU').to_timestamp('W-THU')
        elif key == 5:
            week_dates = df.index.to_period('W-FRI').to_timestamp('W-FRI')
        elif key == 6:
            week_dates = df.index.to_period('W').to_timestamp('W')
        elif key == 7:
            week_dates = df.index.to_period('W-SUN').to_timestamp('W-SUN')

        # 使用 asfreq 方法将数据重采样到需要的周数
        df['week_group'] = week_dates

        return df


    # 按周进行分组 获取数据
    def W_group(self, df, date_name='下单日期'):
        '''
        计算每周的数据
        :param df: 传入的带有自定义时间频率的df
        :return: 返回按周分组后的周数据
        '''
        # 获取每周的数据，排除不足七天的数据
        df['week_day'] = df['week_group'].value_counts()
        df = df.bfill(axis=0)
        df = df[df['week_day'] == 7]

        # 按每周进行分组并求和
        weekly_sum = df.groupby('week_group').sum()

        # 获取最小和最大日期 pd.to_datetime(weekly_sum['min_date']).dt.strftime('%m%d')
        df = df.reset_index()
        min_date = pd.to_datetime(df.groupby('week_group')[date_name].min()).dt.strftime('%Y%m%d')
        max_date = pd.to_datetime(df.groupby('week_group')[date_name].max()).dt.strftime('%Y%m%d')
        dates = min_date.astype(str) + '-' + max_date.astype(str)
        weekly_sum = weekly_sum.reset_index()
        weekly_sum['week_group'] = weekly_sum['week_group'].map(dates, na_action='ignore')

        return weekly_sum


    # 计算每周的总体数据
    def week_data_group(self, df, key):
        '''
        计算每周的总体周数据
        :param df: 总体数据的df
        :param key: 表示周几的参数
        :return: 返回周数据
        '''
        # 使用自定义的 resampler 函数
        weekly_sum = self.W_group(self.custom_weekly_resampler(df, key))
        # 计算每个指标的环比
        # 总体周数据
        weekly_sum['去重订单数环比'] = weekly_sum['去重订单数'].pct_change() * 100
        weekly_sum['去重订单数环比'] = weekly_sum['去重订单数环比'].apply(lambda x: f'{x:.2f}%')

        weekly_sum['进件数环比'] = weekly_sum['进件数'].pct_change() * 100
        weekly_sum['进件数环比'] = weekly_sum['进件数环比'].apply(lambda x: f'{x:.2f}%')

        weekly_sum['出库环比'] = weekly_sum['出库'].pct_change() * 100
        weekly_sum['出库环比'] = weekly_sum['出库环比'].apply(lambda x: f'{x:.2f}%')

        weekly_sum['预授权通过率'] = weekly_sum['进件数'] / weekly_sum['去重订单数'] * 100
        weekly_sum['预授权率环比差值'] = weekly_sum['预授权通过率'].diff()

        weekly_sum['预授权通过率'] = weekly_sum['预授权通过率'].apply(lambda x: f'{x:.2f}%')
        weekly_sum['预授权率环比差值'] = weekly_sum['预授权率环比差值'].apply(lambda x: f'{x:.2f}%')

        weekly_sum['进件出库率'] = weekly_sum['出库'] / weekly_sum['进件数'] * 100
        weekly_sum['进件出库率环比差值'] = weekly_sum['进件出库率'].diff()

        weekly_sum['进件出库率'] = weekly_sum['进件出库率'].apply(lambda x: f'{x:.2f}%')
        weekly_sum['进件出库率环比差值'] = weekly_sum['进件出库率环比差值'].apply(lambda x: f'{x:.2f}%')

        weekly_sum['订单出库率'] = weekly_sum['出库'] / weekly_sum['去重订单数'] * 100
        weekly_sum['订单出库率环比差值'] = weekly_sum['订单出库率'].diff()

        weekly_sum['订单出库率'] = weekly_sum['订单出库率'].apply(lambda x: f'{x:.2f}%')
        weekly_sum['订单出库率环比差值'] = weekly_sum['订单出库率环比差值'].apply(lambda x: f'{x:.2f}%')

        weekly_sum = weekly_sum[['week_group', '去重订单数', '去重订单数环比', '进件数', '进件数环比', '预授权通过率','预授权率环比差值', '出库',
                '出库环比', '进件出库率', '进件出库率环比差值', '订单出库率', '订单出库率环比差值']]

        return weekly_sum


    # 计算每周各渠道的周数据
    def week_data_channel_group(self, df, df_Channel, key):
        '''
        计算每周各渠道的周数据
        :param df: 表示各渠道数据的df
        :param key: 表示周几的参数
        :return: 返回每周的周数据
        '''
        weekly_sum = self.week_data_group(df, key)
        weekly_sum_channel = self.W_group(self.custom_weekly_resampler(df_Channel, key))
        # 渠道周数据
        weekly_sum_channel['预授权通过率'] = weekly_sum_channel['进件数'] / weekly_sum_channel['去重订单数'] * 100
        weekly_sum_channel['预授权通过率'] = weekly_sum_channel['预授权通过率'].apply(lambda x: f'{x:.2f}%')

        weekly_sum_channel['订单占比'] = weekly_sum_channel['去重订单数'] / weekly_sum['去重订单数'] * 100
        weekly_sum_channel['订单占比'] = weekly_sum_channel['订单占比'].apply(lambda x: f'{x:.2f}%')

        weekly_sum_channel['进件占比'] = weekly_sum_channel['进件数'] / weekly_sum['进件数'] * 100
        weekly_sum_channel['进件占比'] = weekly_sum_channel['进件占比'].apply(lambda x: f'{x:.2f}%')

        weekly_sum_channel['出库占比'] = weekly_sum_channel['出库'] / weekly_sum['出库'] * 100
        weekly_sum_channel['出库占比'] = weekly_sum_channel['出库占比'].apply(lambda x: f'{x:.2f}%')

        weekly_sum_channel['进件出库转化率'] = weekly_sum_channel['出库'] / weekly_sum_channel['进件数'] * 100
        weekly_sum_channel['进件出库转化率'] = weekly_sum_channel['进件出库转化率'].apply(lambda x: f'{x:.2f}%')

        weekly_sum_channel['订单出库率'] = weekly_sum_channel['出库'] / weekly_sum_channel['去重订单数'] * 100
        weekly_sum_channel['订单出库率'] = weekly_sum_channel['订单出库率'].apply(lambda x: f'{x:.2f}%')

        return weekly_sum_channel


    # 计算免审，免押周数据
    def week_data_m_group(self, df, key, keys):
        '''
        计算每周的免审和免押数据
        :param df: 免审和免押数据df
        :param key: 表示周几
        :param keys: 表示免押或免审的关键参数
        :return: 返回按周汇总后的数据
        '''
        week_sum_m = self.W_group(self.custom_weekly_resampler(df, key))
        if keys == '免审':
            week_sum_m['免审进件占比'] = week_sum_m['免审进件'] / week_sum_m['总体进件'] * 100
            week_sum_m['免审进件占比'] = week_sum_m['免审进件占比'].apply(lambda x: f'{x:.2f}%')

            week_sum_m['芝麻租物免审进件占比'] = week_sum_m['芝麻租物免审进件'] / week_sum_m['芝麻租物进件'] * 100
            week_sum_m['芝麻租物免审进件占比'] = week_sum_m['芝麻租物免审进件占比'].apply(lambda x: f'{x:.2f}%')

            week_sum_m['免审转化率'] = week_sum_m['免审出库'] / week_sum_m['免审进件'] * 100
            week_sum_m['免审转化率'] = week_sum_m['免审转化率'].apply(lambda x: f'{x:.2f}%')

            week_sum_m['免审出库占比'] = week_sum_m['免审出库'] / week_sum_m['总体出库'] * 100
            week_sum_m['免审出库占比'] = week_sum_m['免审出库占比'].apply(lambda x: f'{x:.2f}%')

            week_sum_m['芝麻租物免审出库占比'] = week_sum_m['芝麻租物免审出库'] / week_sum_m['芝麻租物出库'] * 100
            week_sum_m['芝麻租物免审出库占比'] = week_sum_m['芝麻租物免审出库占比'].apply(lambda x: f'{x:.2f}%')

            return week_sum_m
        else:
            week_sum_m['强拒比例'] = week_sum_m['机审强拒'] / week_sum_m['进件数'] * 100
            week_sum_m['强拒比例'] = week_sum_m['强拒比例'].apply(lambda x: f'{x:.2f}%')

            week_sum_m['风控通过率'] = week_sum_m['风控通过件'] / week_sum_m['进件数'] * 100
            week_sum_m['风控通过率'] = week_sum_m['风控通过率'].apply(lambda x: f'{x:.2f}%')

            week_sum_m["进件出库率"] = week_sum_m["出库"] / week_sum_m["进件数"]
            week_sum_m["进件出库率"] = week_sum_m["进件出库率"].apply(lambda x: format(x, ".2%"))


            week_sum_m["取消率"] = week_sum_m["客户取消"] / week_sum_m["进件数"]
            week_sum_m["取消率"] = week_sum_m["取消率"].apply(lambda x: format(x, ".2%"))

            week_sum_m['人审转化率'] = week_sum_m['出库'] / week_sum_m['机审通过件'] * 100
            week_sum_m['人审转化率'] = week_sum_m['人审转化率'].apply(lambda x: f'{x:.2f}%')

            week_sum_m["人审拒绝率"] = week_sum_m["人审拒绝"] / week_sum_m["进件数"]
            week_sum_m["人审拒绝率"] = week_sum_m["人审拒绝率"].apply(lambda x: format(x, ".2%"))

            week_sum_m["出库前强拒比例"] = week_sum_m["出库前风控强拒"] / week_sum_m["进件数"]
            week_sum_m["出库前强拒比例"] = week_sum_m["出库前强拒比例"].apply(lambda x: format(x, ".2%"))

            week_sum_m["无法联系占比"] = week_sum_m["无法联系"] / week_sum_m["进件数"]
            week_sum_m["无法联系占比"] = week_sum_m["无法联系占比"].apply(lambda x: format(x, ".2%"))

            return week_sum_m


    # 计算自然周数据
    def week_data_group_all(self, df, key):
        '''
        计算每周的总体周数据
        :param df: 总体数据的df
        :param key: 表示周几的参数
        :return: 返回周数据
        '''
        # 使用自定义的 resampler 函数
        df_group = self.W_group(self.custom_weekly_resampler(df, key))
        for status_type in df_group.columns.to_list()[1:-1]:
            if status_type == '前置拦截':
                df_group["拦截率"] = df_group["前置拦截"] / df_group["去重订单数"]
                df_group["拦截率"] = df_group["拦截率"].apply(lambda x: format(x, ".2%"))
            elif status_type == '进件前取消':
                df_group["进件前取消率"] = df_group["进件前取消"] / df_group["去重订单数"]
                df_group["进件前取消率"] = df_group["进件前取消率"].apply(lambda x: format(x, ".2%"))
            elif status_type == '进件数' or status_type == '进件':
                df_group['预授权通过率'] = df_group["进件数"] / df_group["去重订单数"]
                df_group["预授权通过率"] = df_group["预授权通过率"].apply(lambda x: format(x, ".2%"))
            elif status_type == '出库':
                df_group["进件出库率"] = df_group["出库"] / df_group["进件数"]
                df_group["进件出库率"] = df_group["进件出库率"].apply(lambda x: format(x, ".2%"))
                df_group["订单出库率"] = df_group["出库"] / df_group["去重订单数"]
                df_group["订单出库率"] = df_group["订单出库率"].apply(lambda x: format(x, ".2%"))
                df_group["人审转化率"] = df_group["出库"] / df_group["机审通过件"]
                df_group["人审转化率"] = df_group["人审转化率"].apply(lambda x: format(x, ".2%"))
            elif status_type == '去重订单数':
                pass
            else:
                df_group[f"{status_type}比例"] = df_group[f'{status_type}'] / df_group["进件数"]
                df_group[f"{status_type}比例"] = df_group[f"{status_type}比例"].apply(lambda x: format(x, ".2%"))


        return df_group

    # 统计商家周数据
    def week_data_group_merchant(self, df, merchant_name, model, key):
        '''
        统计商家一个自然周的周期数据
        :param df: 商家的数据
        :param key: 统计周期
        :return: 返回统计好的周期数据
        '''
        df_merchant = self.all_models.merchant_names(df, merchant_name, model)
        df_merchant = self.clean.continuous_dates(df_merchant).rename_axis('下单日期')
        df_merchant = df_merchant[['去重订单数', '进件数', '进件前取消', '前置拦截', '机审强拒', '人审拒绝', '客户取消', '无法联系', '出库前风控强拒', '待审核', '出库']]
        # 使用自定义的 resampler 函数
        df_group = self.W_group(self.custom_weekly_resampler(df_merchant, key))

        df_group["拦截率"] = df_group["前置拦截"] / df_group["去重订单数"]
        df_group["拦截率"] = df_group["拦截率"].apply(lambda x: format(x, ".2%"))

        df_group["进件前取消率"] = df_group["进件前取消"] / df_group["去重订单数"]
        df_group["进件前取消率"] = df_group["进件前取消率"].apply(lambda x: format(x, ".2%"))

        df_group['预授权通过率'] = df_group["进件数"] / df_group["去重订单数"]
        df_group["预授权通过率"] = df_group["预授权通过率"].apply(lambda x: format(x, ".2%"))

        df_group["强拒比例"] = df_group["机审强拒"] / df_group["进件数"]
        df_group["强拒比例"] = df_group["强拒比例"].apply(lambda x: format(x, ".2%"))

        df_group["机审通过件"] = df_group["进件数"] - df_group["机审强拒"]

        df_group["风控通过件"] = df_group["进件数"] - df_group["机审强拒"] - df_group["人审拒绝"]
        df_group["风控通过率"] = df_group["风控通过件"] / df_group["进件数"]
        df_group["风控通过率"] = df_group["风控通过率"].apply(lambda x: format(x, ".2%"))

        df_group["取消率"] = df_group["客户取消"] / df_group["进件数"]
        df_group["取消率"] = df_group["取消率"].apply(lambda x: format(x, ".2%"))

        df_group["人审拒绝率"] = df_group["人审拒绝"] / df_group["进件数"]
        df_group["人审拒绝率"] = df_group["人审拒绝率"].apply(lambda x: format(x, ".2%"))

        df_group["进件出库率"] = df_group["出库"] / df_group["进件数"]
        df_group["进件出库率"] = df_group["进件出库率"].apply(lambda x: format(x, ".2%"))

        df_group["订单出库率"] = df_group["出库"] / df_group["去重订单数"]
        df_group["订单出库率"] = df_group["订单出库率"].apply(lambda x: format(x, ".2%"))

        return df_group




# 风控类
class Risk_Data:
    # 前置拦截
    def pre_reject(self, df, model):
        '''
        前置拦截命中
        :param df: 全部数据的df
        :param model: 分组的参数
        :return: 返回命中的规则数据
        '''
        # 年龄超过49岁或低于18岁
        df_nl = df[(df["拒绝理由"].notnull()) & (df["status2"] == "订单取消") & (
            df["拒绝理由"].str.contains("年龄超过49岁或低于18岁"))].groupby(model).agg(
            {"order_id": np.size}).reset_index()
        df_nl = df_nl.rename(columns={"order_id": "年龄超过49岁或低于18岁"})
        # 法院被执行人
        df_zx = df[(df["拒绝理由"].notnull()) & (df["status2"] == "订单取消") & (
            df["拒绝理由"].str.contains(r"被执行人|司法高院有限制消费案件记录|司法高院有失信案件记录"))].groupby(model).agg({"order_id": np.size}).reset_index()
        df_zx = df_zx.rename(columns={"order_id": "命中法院失信/限高被执行人"})

        df_nl_zx = pd.merge(df_nl, df_zx, on=model, how='outer')

        df_xs = df[(df["拒绝理由"].notnull()) & (df["status2"] == "订单取消") & (
            df["拒绝理由"].str.contains(r"命中刑事案件|司法高院个人有刑事案件"))].groupby(model).agg({"order_id": np.size}).reset_index()
        df_xs = df_xs.rename(columns={"order_id": "命中刑事案件"})

        df_zx_xs = pd.merge(df_nl_zx, df_xs, on=model, how='outer')

        # 命中借贷纠纷
        df_jf = df[(df["拒绝理由"].notnull()) & (df["status2"] == "订单取消") & (
            df["拒绝理由"].str.contains(r"命中借贷纠纷|司法高院个人涉诉案件数|司法高院个人有未结案被告金额过大|司法高院个人有非诉讼保全审查案件|司法高院个人有执行案件|司法高院个人有强制清算与破产案件"))].groupby(model).agg({"order_id": np.size}).reset_index()
        df_jf = df_jf.rename(columns={"order_id": "命中借贷纠纷"})

        df_zx_xs_jf = pd.merge(df_zx_xs, df_jf, on=model, how='outer')

        # 命中融安分低于680强拒
        df_ra = df[(df["拒绝理由"].notnull()) & (df["status2"] == "订单取消") & (
            df["拒绝理由"].str.contains("命中融安分"))].groupby(model).agg(
            {"order_id": np.size}).reset_index()
        df_ra = df_ra.rename(columns={"order_id": "命中融安分低于680"})

        df_zx_xs_jf_ra = pd.merge(df_zx_xs_jf, df_ra, on=model, how='outer')

        # 命中特殊名单验证-高风险
        df_gf = df[(df["拒绝理由"].notnull()) & (df["status2"] == "订单取消") & (
            df["拒绝理由"].str.contains("命中特殊名单验证-高风险"))].groupby(model).agg(
            {"order_id": np.size}).reset_index()
        df_gf = df_gf.rename(columns={"order_id": "命中高风险名单"})

        df_zx_xs_jf_ra_gf = pd.merge(df_zx_xs_jf_ra, df_gf, on=model, how='outer')

        # 身份证号码命中黑名单
        df_hm = df[(df["拒绝理由"].notnull()) & (df["status2"] == "订单取消") & (
            df["拒绝理由"].str.contains("身份证"))].groupby(model).agg({"order_id": np.size}).reset_index()
        df_hm = df_hm.rename(columns={"order_id": "身份证命中黑名单"})

        df_zx_xs_jf_ra_gf_df_hm = pd.merge(df_zx_xs_jf_ra_gf, df_hm, on=model, how='outer')

        # 智融分低于458
        df_zr = df[(df["拒绝理由"].notnull()) & (df["status2"] == "订单取消") & (
            df["拒绝理由"].str.contains("冰鉴火眸分"))].groupby(model).agg({"order_id": np.size}).reset_index()
        df_zr = df_zr.rename(columns={"order_id": "冰鉴火眸分<500"})

        df_zx_xs_jf_ra_gf_df_hm_zr = pd.merge(df_zx_xs_jf_ra_gf_df_hm, df_zr, on=model, how='outer')

        # 命中强拒加入临时黑名单
        df_lshm = df[(df["拒绝理由"].notnull()) & (df["status2"] == "订单取消") & (
            df["拒绝理由"].str.contains("命中强拒加入临时黑名单"))].groupby(model).agg(
            {"order_id": np.size}).reset_index()
        df_lshm = df_lshm.rename(columns={"order_id": "命中强拒加入临时黑名单"})

        df_zx_xs_jf_ra_gf_df_hm_zr_lshm = pd.merge(df_zx_xs_jf_ra_gf_df_hm_zr, df_lshm, on=model, how='outer')

        # 当日下单次数大于等于5次
        df_cf = df[(df["拒绝理由"].notnull()) & (df["status2"] == "订单取消") & (
            df["拒绝理由"].str.contains("当日下单次数大于等于5次"))].groupby(model).agg(
            {"order_id": np.size}).reset_index()
        df_cf = df_cf.rename(columns={"order_id": "当日下单次数大于等于5次"})

        df_zx_xs_jf_ra_gf_df_hm_zr_lshm_cf = pd.merge(df_zx_xs_jf_ra_gf_df_hm_zr_lshm, df_cf, on=model, how='outer')

        # 当月下单次数大于等于10次
        df_cf2 = df[(df["total_describes"].notnull()) & (df["status2"] == "订单取消") & (
            df["total_describes"].str.contains("当月下单次数大于等于10次"))].groupby(model).agg(
            {"order_id": np.size}).reset_index()
        df_cf2 = df_cf2.rename(columns={"order_id": "当月下单次数大于等于10次"})

        f_zx_xs_jf_ra_gf_df_hm_zr_lshm_cf_cf2 = pd.merge(df_zx_xs_jf_ra_gf_df_hm_zr_lshm_cf, df_cf2, on=model, how='outer')

        # 命中特殊地区 新疆|西藏
        df_diqu = df[(df["total_describes"].notnull()) & (df["status2"] == "订单取消") & (
            df["total_describes"].str.contains("命中特殊地区"))].groupby(model).agg(
            {"order_id": np.size}).reset_index()
        df_diqu = df_diqu.rename(columns={"order_id": "命中特殊地区 新疆|西藏"})
        f_zx_xs_jf_ra_gf_df_hm_zr_lshm_cf_cf2_diqu = pd.merge(f_zx_xs_jf_ra_gf_df_hm_zr_lshm_cf_cf2, df_diqu, on=model, how='outer')

        # 评分等级低于D
        df_pf = df[(df["拒绝理由"].notnull()) & (df["status2"] == "订单取消") & (
            df["拒绝理由"].str.contains("评分等级低于D"))].groupby(model).agg(
            {"order_id": np.size}).reset_index()
        df_pf = df_pf.rename(columns={"order_id": "评分等级低于D"})
        df_all = pd.merge(f_zx_xs_jf_ra_gf_df_hm_zr_lshm_cf_cf2_diqu, df_pf, on=model,how='outer')

        # 命中TD212强拒
        df_td212 = df[(df["拒绝理由"].notnull()) & (df["status2"] == "订单取消") & (
            df["拒绝理由"].str.contains("命中TD212强拒"))].groupby(model).agg(
            {"order_id": np.size}).reset_index()
        df_td212 = df_td212.rename(columns={"order_id": "命中TD212强拒"})
        df_all1 = pd.merge(df_all, df_td212, on=model, how='outer')

        return df_all1

    def pre_reject_dy(self, df, model):
        # 年龄超过49岁或低于18岁
        df_nl = df[
            (df["total_describes"].notnull()) & (df["total_describes"].str.contains("年龄超过49岁或低于18岁"))].groupby(
            [model]).agg({"order_id": np.size}).reset_index()
        df_nl = df_nl.rename(columns={"order_id": "年龄超过49岁或低于18岁"})
        # 法院被执行人
        df_zx = df[(df["total_describes"].notnull()) & (df["total_describes"].str.contains("被执行人"))].groupby(
            [model]).agg({"order_id": np.size}).reset_index()
        df_zx = df_zx.rename(columns={"order_id": "命中法院失信/限高被执行人"})

        df_nl_zx = pd.merge(df_nl, df_zx, on=model, how='outer')

        df_xs = df[(df["total_describes"].notnull()) & (df["total_describes"].str.contains("命中刑事案件"))].groupby(
            [model]).agg({"order_id": np.size}).reset_index()
        df_xs = df_xs.rename(columns={"order_id": "命中刑事案件"})

        df_zx_xs = pd.merge(df_nl_zx, df_xs, on=model, how='outer')

        # 命中借贷纠纷
        df_jf = df[(df["total_describes"].notnull()) & (df["total_describes"].str.contains("命中借贷纠纷"))].groupby(
            [model]).agg({"order_id": np.size}).reset_index()
        df_jf = df_jf.rename(columns={"order_id": "命中借贷纠纷"})

        df_zx_xs_jf = pd.merge(df_zx_xs, df_jf, on=model, how='outer')

        # 命中融安分低于680强拒
        df_ra = df[
            (df["total_describes"].notnull()) & (df["total_describes"].str.contains("命中融安分"))].groupby(
            [model]).agg({"order_id": np.size}).reset_index()
        df_ra = df_ra.rename(columns={"order_id": "命中融安分低于680"})

        df_zx_xs_jf_ra = pd.merge(df_zx_xs_jf, df_ra, on=model, how='outer')

        # 命中特殊名单验证-高风险
        df_gf = df[(df["total_describes"].notnull()) & (
            df["total_describes"].str.contains("命中特殊名单验证-高风险"))].groupby([model]).agg(
            {"order_id": np.size}).reset_index()
        df_gf = df_gf.rename(columns={"order_id": "命中高风险名单"})

        df_zx_xs_jf_ra_gf = pd.merge(df_zx_xs_jf_ra, df_gf, on=model, how='outer')

        # 身份证号码命中黑名单
        df_hm = df[(df["total_describes"].notnull()) & (df["total_describes"].str.contains("身份证"))].groupby(
            [model]).agg({"order_id": np.size}).reset_index()
        df_hm = df_hm.rename(columns={"order_id": "身份证命中黑名单"})

        df_zx_xs_jf_ra_gf_df_hm = pd.merge(df_zx_xs_jf_ra_gf, df_hm, on=model, how='outer')

        # 智融分低于458
        df_zr = df[(df["total_describes"].notnull()) & (df["total_describes"].str.contains("冰鉴火眸分"))].groupby(
            [model]).agg({"order_id": np.size}).reset_index()
        df_zr = df_zr.rename(columns={"order_id": "冰鉴火眸分<500"})

        df_zx_xs_jf_ra_gf_df_hm_zr = pd.merge(df_zx_xs_jf_ra_gf_df_hm, df_zr, on=model, how='outer')

        # 命中强拒加入临时黑名单
        df_lshm = df[
            (df["total_describes"].notnull()) & (df["total_describes"].str.contains("命中强拒加入临时黑名单"))].groupby(
            [model]).agg({"order_id": np.size}).reset_index()
        df_lshm = df_lshm.rename(columns={"order_id": "命中强拒加入临时黑名单"})

        df_zx_xs_jf_ra_gf_df_hm_zr_lshm = pd.merge(df_zx_xs_jf_ra_gf_df_hm_zr, df_lshm, on=model, how='outer')

        # 当日下单次数大于等于5次
        df_cf = df[(df["total_describes"].notnull()) & (
            df["total_describes"].str.contains("当日下单次数大于等于5次"))].groupby([model]).agg(
            {"order_id": np.size}).reset_index()
        df_cf = df_cf.rename(columns={"order_id": "当日下单次数大于等于5次"})

        df_zx_xs_jf_ra_gf_df_hm_zr_lshm_cf = pd.merge(df_zx_xs_jf_ra_gf_df_hm_zr_lshm, df_cf, on=model, how='outer')

        # 当月下单次数大于等于10次
        df_cf2 = df[(df["total_describes"].notnull()) & (
            df["total_describes"].str.contains("当月下单次数大于等于10次"))].groupby([model]).agg(
            {"order_id": np.size}).reset_index()
        df_cf2 = df_cf2.rename(columns={"order_id": "当月下单次数大于等于10次"})

        f_zx_xs_jf_ra_gf_df_hm_zr_lshm_cf_cf2 = pd.merge(df_zx_xs_jf_ra_gf_df_hm_zr_lshm_cf, df_cf2, on=model,how='outer')

        # 命中特殊地区 新建|西藏
        df_diqu = df[(df["total_describes"].notnull()) & (df["total_describes"].str.contains("命中特殊地区"))].groupby(
            [model]).agg({"order_id": np.size}).reset_index()
        df_diqu = df_diqu.rename(columns={"order_id": "命中特殊地区 新疆|西藏"})
        f_zx_xs_jf_ra_gf_df_hm_zr_lshm_cf_cf2_diqu = pd.merge(f_zx_xs_jf_ra_gf_df_hm_zr_lshm_cf_cf2, df_diqu, on=model, how='outer')

        return f_zx_xs_jf_ra_gf_df_hm_zr_lshm_cf_cf2_diqu


    # 机审强拒
    def model_reject(self, df_j, model):
        '''
        机审命中规则
        :param df_j: 进件后的数据
        :param model: 分组的参数
        :return: 返回命中规则的数据
        '''
        # 评分等级D且综合风险等级为3
        df_j1 = df_j[df_j["拒绝理由"].str.contains(pat="评分等级D且综合风险等级为3", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j1 = df_j1.rename(columns={"order_id": "评分等级D且综合风险等级为3"})

        # 综合风险等级为3且非免押
        df_j2 = df_j[df_j["拒绝理由"].str.contains(pat="综合风险等级为3", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j2 = df_j2.rename(columns={"order_id": "综合风险等级为3"})

        df_j12 = pd.merge(df_j1, df_j2, on=model, how='outer')

        # 法院被执行人
        df_j3 = df_j[df_j["拒绝理由"].str.contains(pat="被执行人", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j3 = df_j3.rename(columns={"order_id": "命中法院失信/限高被执行人"})

        df_j123 = pd.merge(df_j12, df_j3, on=model, how='outer')

        df_j4 = df_j[df_j["拒绝理由"].str.contains(pat="命中刑事案件", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j4 = df_j4.rename(columns={"order_id": "命中刑事案件"})

        df_j1234 = pd.merge(df_j123, df_j4, on=model, how='outer')

        # 命中借贷纠纷
        df_j5 = df_j[df_j["拒绝理由"].str.contains(pat="命中借贷纠纷", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j5 = df_j5.rename(columns={"order_id": "命中借贷纠纷"})

        df_j12345 = pd.merge(df_j1234, df_j5, on=model, how='outer')

        # 命中融安分低于680强拒
        df_j6 = df_j[df_j["拒绝理由"].str.contains(pat="命中融安分低于680", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j6 = df_j6.rename(columns={"order_id": "命中融安分低于680"})

        df_j123456 = pd.merge(df_j12345, df_j6, on=model, how='outer')

        # 命中特殊名单验证-高风险
        df_j7 = df_j[df_j["拒绝理由"].str.contains(pat="命中特殊名单验证-高风险", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j7 = df_j7.rename(columns={"order_id": "命中高风险名单"})

        df_j1234567 = pd.merge(df_j123456, df_j7, on=model, how='outer')

        # 命中风险勘测名单
        df_j8 = df_j[df_j["拒绝理由"].str.contains(pat="风险勘测", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j8 = df_j8.rename(columns={"order_id": "命中风险勘测"})

        df_j12345678 = pd.merge(df_j1234567, df_j8, on=model, how='outer')

        # 蚁盾分>=90
        df_j9 = df_j[df_j["拒绝理由"].str.contains(pat="蚁盾分>=80", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j9 = df_j9.rename(columns={"order_id": "蚁盾分>=80"})

        df_j123456789 = pd.merge(df_j12345678, df_j9, on=model, how='outer')

        # 30天多头>=10且90天多头>=31
        # df_j101 = df_j[df_j["拒绝理由"].str.contains(pat="命中30天多头>9并且90天多头>30", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        # df_j101 = df_j101.rename(columns={"order_id": "命中30天多头>9并且90天多头>30"})
        # df_j123456789101 = pd.merge(df_j123456789, df_j101, on=model, how='outer')

        df_j10 = df_j[df_j["拒绝理由"].str.contains(pat="命中30天多头>13并且90天多头>39", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j10 = df_j10.rename(columns={"order_id": "命中30天多头>13并且90天多头>39"})


        df_j12345678910 = pd.merge(df_j123456789, df_j10, on=model, how='outer')

        # 履约历史等级1且为搜索渠道
        df_j11 = df_j[df_j["拒绝理由"].str.contains(pat="履约历史等级1", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j11 = df_j11.rename(columns={"order_id": "履约历史等级1且为搜索渠道"})

        df_j1234567891011 = pd.merge(df_j12345678910, df_j11, on=model, how='outer')

        # 云商分低于496
        df_j12 = df_j[df_j["拒绝理由"].str.contains(pat=r"云商分低于496|云商分低于476") == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j12 = df_j12.rename(columns={"order_id": "云商分低于496"})

        df_j123456789101112 = pd.merge(df_j1234567891011, df_j12, on=model, how='outer')

        # 评分等级低于D
        df_j12 = df_j[df_j["拒绝理由"].str.contains(pat="评分等级低于D", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j12 = df_j12.rename(columns={"order_id": "评分等级低于D"})

        df_dj = pd.merge(df_j123456789101112, df_j12, on=model, how='outer')

        # 评分等级D且非免押客户
        df_j13 = df_j[df_j["拒绝理由"].str.contains(pat="评分等级D且非免押客户", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j13 = df_j13.rename(columns={"order_id": "评分等级D且非免押客户"})

        df_dj_fmy = pd.merge(df_dj, df_j13, on=model, how='outer')

        # 非免押客户
        df_j14 = df_j[df_j["拒绝理由"].str.contains(pat="命中非免押用户拒绝", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j14 = df_j14.rename(columns={"order_id": "非免押用户"})

        df_dj_last1 = pd.merge(df_dj_fmy, df_j14, on=model, how='outer')

        # 命中自有模型强拒
        df_j15 = df_j[df_j["拒绝理由"].str.contains(pat=r"命中自有模型强拒|命中模型2501强拒") == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j15 = df_j15.rename(columns={"order_id": "命中自有模型强拒"})

        df_dj_last2 = pd.merge(df_dj_last1, df_j15, on=model, how='outer')

        # 非芝麻租物进件且履约历史等级=1强拒
        df_j16 = df_j[df_j["拒绝理由"].str.contains(pat="履约历史等级=1强拒", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j16 = df_j16.rename(columns={"order_id": "非芝麻租物进件且履约历史等级=1强拒"})

        df_dj_last3 = pd.merge(df_dj_last2, df_j16, on=model, how='outer')

        # 决策树组合策略 14
        df_j17 = df_j[df_j["拒绝理由"].str.contains(pat="命中策略240703_14强拒", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j17 = df_j17.rename(columns={"order_id": "命中策略240703_14强拒"})

        df_dj_last4 = pd.merge(df_dj_last3, df_j17, on=model, how='outer')

        # 决策树组合策略 4
        df_j18 = df_j[df_j["拒绝理由"].str.contains(pat="命中策略240703_4强拒", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j18 = df_j18.rename(columns={"order_id": "命中策略240703_4强拒"})

        df_dj_last5 = pd.merge(df_dj_last4, df_j18, on=model, how='outer')

        # 决策树组合策略 10
        df_j19 = df_j[df_j["拒绝理由"].str.contains(pat="命中策略240703_10强拒", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j19 = df_j19.rename(columns={"order_id": "命中策略240703_10强拒"})

        df_dj_last6 = pd.merge(df_dj_last5, df_j19, on=model, how='outer')

        # 命中策略strategy_240801强拒
        df_j20 = df_j[df_j["拒绝理由"].str.contains(pat="命中策略strategy_240801强拒", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j20 = df_j20.rename(columns={"order_id": "命中拒绝_通过加强包240801强拒"})

        df_dj_last7 = pd.merge(df_dj_last6, df_j20, on=model, how='outer')

        # 蚂蚁数控风险等级=996强拒
        df_j21 = df_j[
            df_j["total_describes"].str.contains(pat="蚂蚁数控风险等级=996强拒", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j21 = df_j21.rename(columns={"order_id": "蚂蚁数控风险等级=996强拒"})

        df_dj_last8 = pd.merge(df_dj_last7, df_j21, on=model, how='outer')

        # 蚂蚁数控风险等级=998强拒
        df_j22 = df_j[
            df_j["total_describes"].str.contains(pat="蚂蚁数控风险等级=998强拒", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j22 = df_j22.rename(columns={"order_id": "蚂蚁数控风险等级=998强拒"})

        df_dj_last9 = pd.merge(df_dj_last8, df_j22, on=model, how='outer')


        df_j23 = df_j[df_j["result"].str.contains(pat="命中出库前风控流强拒", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j23 = df_j23.rename(columns={"order_id": "命中出库前风控流强拒"})

        df_dj_last10 = pd.merge(df_dj_last9, df_j23, on=model, how='outer')

        # 命中策略strategy_240829强拒  strategy_240829
        df_j24 = df_j[df_j["拒绝理由"].str.contains(pat="命中策略strategy_240829强拒", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j24 = df_j24.rename(columns={"order_id": "命中抖音_240829策略强拒"})

        df_dj_last11 = pd.merge(df_dj_last10, df_j24, on=model, how='outer')

        df_j25 = df_j[df_j["拒绝理由"].str.contains(pat="命中策略strategy_240927强拒", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j25 = df_j25.rename(columns={"order_id": "240927策略强拒"})

        df_dj_last12 = pd.merge(df_dj_last11, df_j25, on=model, how='outer')

        # 云商分低于476
        # df_j26 = df_j[df_j["拒绝理由"].str.contains(pat="云商分低于476", regex=False) == True].groupby(model).agg(
        #     {"order_id": np.size}).reset_index()
        # df_j26 = df_j26.rename(columns={"order_id": "云商分低于476"})

        # df_dj_last13 = pd.merge(df_dj_last12, df_j26, on=model, how='outer')

        return df_dj_last12

    # 2025-04-29 更新
    def model_reject2(self, df_j, model):
        '''
        机审命中规则
        :param df_j: 进件后的数据
        :param model: 分组的参数
        :return: 返回命中规则的数据
        '''

        # 命中非免押用户强拒
        df_j1 = df_j[df_j["拒绝理由"].str.contains(pat="命中非免押用户拒绝", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j1 = df_j1.rename(columns={"order_id": "命中非免押用户拒绝"})

        # 非芝麻租物进件且履约历史等级=1强拒
        df_j2 = df_j[df_j["拒绝理由"].str.contains(pat="履约历史等级", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j2 = df_j2.rename(columns={"order_id": "非芝麻租物进件且履约历史等级=1强拒"})
        df_j12 = pd.merge(df_j1, df_j2, on=model, how='outer')

        # 综合风险等级为3
        df_j4 = df_j[df_j["拒绝理由"].str.contains(pat="综合风险等级为3", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j4 = df_j4.rename(columns={"order_id": "综合风险等级为3"})
        df_j1234 = pd.merge(df_j12, df_j4, on=model, how='outer')

        # 蚁盾分强拒
        df_j5 = df_j[df_j["拒绝理由"].str.contains(pat="蚁盾分>", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j5 = df_j5.rename(columns={"order_id": "命中蚁盾分强拒"})
        df_j12345 = pd.merge(df_j1234, df_j5, on=model, how='outer')

        # 命中法院失信/限高被执行人
        df_j7 = df_j[df_j["拒绝理由"].str.contains(pat=r"被执行人|失信人|司法高院有限制消费案件记录|司法高院有失信案件记录") == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j7 = df_j7.rename(columns={"order_id": "命中法院失信/限高被执行人"})
        df_j123456 = pd.merge(df_j12345, df_j7, on=model, how='outer')

        # 命中高风险名单
        df_j10 = df_j[df_j["拒绝理由"].str.contains(pat="命中特殊名单验证-高风险强拒", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j10 = df_j10.rename(columns={"order_id": "命中特殊名单验证-高风险强拒"})
        df_j123456789 = pd.merge(df_j123456, df_j10, on=model, how='outer')

        # 风险勘测
        df_j11 = df_j[df_j["拒绝理由"].str.contains(pat="命中百融借贷风险勘测强拒", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j11 = df_j11.rename(columns={"order_id": "命中百融借贷风险勘测强拒"})
        df_j12345678910 = pd.merge(df_j123456789, df_j11, on=model, how='outer')

        # 命中云商分低于496
        df_j12 = df_j[df_j["拒绝理由"].str.contains(pat=r"命中云商分") == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j12 = df_j12.rename(columns={"order_id": "命中云商分强拒"})
        df_j1234567891011 = pd.merge(df_j12345678910, df_j12, on=model, how='outer')

        # 命中融安分
        df_j13 = df_j[df_j["拒绝理由"].str.contains(pat="命中融安分", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j13 = df_j13.rename(columns={"order_id": "命中融安分强拒"})
        df_dj = pd.merge(df_j1234567891011, df_j13, on=model, how='outer')

        # 命中自有模型强拒
        # df_j14 = df_j[(df_j["拒绝理由"].str.contains(pat="模型", regex=False) == True)&(~df_j["拒绝理由"].str.contains(pat="Fico联合规则强拒", regex=False) == True)].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j14 = df_j[(df_j["拒绝理由"].str.contains(pat="模型", regex=False) == True)&(~(df_j["拒绝理由"].str.contains(pat="Fico联合规则强拒", regex=False) == True))].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j14 = df_j14.rename(columns={"order_id": "命中模型强拒"})
        df_dj_last1 = pd.merge(df_dj, df_j14, on=model, how='outer')

        # 命中青云分<500
        df_j15 = df_j[df_j["拒绝理由"].str.contains(pat=r"命中青云分|命中冰鉴青云分") == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j15 = df_j15.rename(columns={"order_id": "命中青云分强拒"})
        df_dj_last2 = pd.merge(df_dj_last1, df_j15, on=model, how='outer')

        # 评分等级低于D
        df_j16 = df_j[df_j["拒绝理由"].str.contains(pat="评分等级低于D", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j16 = df_j16.rename(columns={"order_id": "评分等级低于D"})
        df_dj_last3 = pd.merge(df_dj_last2, df_j16, on=model, how='outer')

        # 评分等级为D且综合风险等级为3
        df_j17 = df_j[df_j["total_describes"].str.contains(pat="评分等级D且综合风险等级为3", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j17 = df_j17.rename(columns={"order_id": "评分等级D且综合风险等级为3"})
        df_dj_last4 = pd.merge(df_dj_last3, df_j17, on=model, how='outer')

        # 评分等级D且非免押客户
        df_j18 = df_j[df_j["total_describes"].str.contains(pat="评分等级D且非免押客户", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j18 = df_j18.rename(columns={"order_id": "评分等级D且非免押客户"})
        df_dj_last5 = pd.merge(df_dj_last4, df_j18, on=model, how='outer')

        # 30天多头>13且90天多头>39
        df_j19 = df_j[df_j["拒绝理由"].str.contains(pat=r"命中30天多头>(\d+)并且90天多头>(\d+)", regex=True) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j19 = df_j19.rename(columns={"order_id": "30天多头>13且90天多头>39"})
        df_dj_last6 = pd.merge(df_dj_last5, df_j19, on=model, how='outer')

        # 命中策略策略240703_4强拒
        df_j20 = df_j[df_j["拒绝理由"].str.contains(pat="命中策略240703_4强拒", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j20 = df_j20.rename(columns={"order_id": "命中策略策略240703_4强拒"})
        df_dj_last7 = pd.merge(df_dj_last6, df_j20, on=model, how='outer')

        # 命中策略240703_10强拒
        df_j21 = df_j[df_j["拒绝理由"].str.contains(pat="命中策略240703_10强拒", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j21 = df_j21.rename(columns={"order_id": "命中策略240703_10强拒"})
        df_dj_last8 = pd.merge(df_dj_last7, df_j21, on=model, how='outer')

        # 命中策略240703_14强拒
        df_j22 = df_j[df_j["拒绝理由"].str.contains(pat="命中策略240703_14强拒", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j22 = df_j22.rename(columns={"order_id": "命中策略240703_14强拒"})
        df_dj_last9 = pd.merge(df_dj_last8, df_j22, on=model, how='outer')

        # 命中策略strategy_240801强拒
        df_j23 = df_j[df_j["拒绝理由"].str.contains(pat="命中策略strategy_240801强拒", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j23 = df_j23.rename(columns={"order_id": "命中策略strategy_240801强拒"})
        df_dj_last10 = pd.merge(df_dj_last9, df_j23, on=model, how='outer')

        # 蚂蚁数控风险等级强拒
        df_j24 = df_j[df_j["total_describes"].str.contains(pat="蚂蚁数控风险等级", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j24 = df_j24.rename(columns={"order_id": "命中蚂蚁数控风险等级强拒"})
        df_dj_last11 = pd.merge(df_dj_last10, df_j24, on=model, how='outer')

        # 命中抖音_240829策略强拒
        df_j27 = df_j[df_j["拒绝理由"].str.contains(pat="命中抖音_240829策略强拒", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j27 = df_j27.rename(columns={"order_id": "命中抖音_240829策略强拒"})
        df_dj_last14 = pd.merge(df_dj_last11, df_j27, on=model, how='outer')

        # 命中策略strategy_240927强拒
        df_j28 = df_j[df_j["拒绝理由"].str.contains(pat="命中策略strategy_240927强拒", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j28 = df_j28.rename(columns={"order_id": "命中策略strategy_240927强拒"})
        df_dj_last15 = pd.merge(df_dj_last14, df_j28, on=model, how='outer')

        # 命中刑事案件_机审
        df_j29 = df_j[(df_j["拒绝理由"].str.contains(pat=r"命中刑事案件|司法高院个人有刑事案件") == True)&(df_j.机审强拒==1)].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j29 = df_j29.rename(columns={"order_id": "命中刑事案件_机审"})
        df_dj_last16 = pd.merge(df_dj_last15, df_j29, on=model, how='outer')

        # 命中借贷纠纷_机审
        df_j30 = df_j[(df_j["拒绝理由"].str.contains(pat=r"命中借贷纠纷|司法高院个人涉诉案件数|司法高院个人有未结案被告金额过大|司法高院个人有非诉讼保全审查案件|司法高院个人有执行案件|司法高院个人有强制清算与破产案件") == True)&(df_j.机审强拒==1)].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j30 = df_j30.rename(columns={"order_id": "命中借贷纠纷_机审"})
        df_dj_last17 = pd.merge(df_dj_last16, df_j30, on=model, how='outer')

        # 命中刑事案件_出库前
        df_j29_2 = df_j[(df_j["拒绝理由"].str.contains(pat="命中刑事案件", regex=False) == True)&(df_j.出库前风控强拒==1)].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j29_2 = df_j29_2.rename(columns={"order_id": "命中刑事案件_出库前"})
        df_dj_last16_2 = pd.merge(df_dj_last17, df_j29_2, on=model, how='outer')

        # 命中借贷纠纷_出库前
        df_j30_2 = df_j[(df_j["拒绝理由"].str.contains(pat="命中借贷纠纷", regex=False) == True)&(df_j.出库前风控强拒==1)].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j30_2 = df_j30_2.rename(columns={"order_id": "命中借贷纠纷_出库前"})
        df_dj_last17_2 = pd.merge(df_dj_last16_2, df_j30_2, on=model, how='outer')

        # 命中极信sc32007分强拒
        df_j31 = df_j[df_j["拒绝理由"].str.contains(pat="命中极信sc32007分强拒", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j31 = df_j31.rename(columns={"order_id": "命中极信sc32007分强拒"})
        df_dj_last18 = pd.merge(df_dj_last17_2, df_j31, on=model, how='outer')

        # 命中银联模型及Fico联合规则强拒
        df_j32 = df_j[df_j["拒绝理由"].str.contains(pat="命中银联模型及Fico联合规则强拒", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j32 = df_j32.rename(columns={"order_id": "命中银联模型及Fico联合规则强拒"})
        df_dj_last19 = pd.merge(df_dj_last18, df_j32, on=model, how='outer')

        # 以下四条为 S量独有

        # 命中蚁盾分2.0强拒
        df_j33 = df_j[df_j["拒绝理由"].str.contains(pat="蚁盾分2.0", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j33 = df_j33.rename(columns={"order_id": "命中蚁盾分2.0强拒"})
        df_dj_last20 = pd.merge(df_dj_last19, df_j33, on=model, how='outer')

        # 命中占融202098联合规则强拒
        df_j34 = df_j[df_j["拒绝理由"].str.contains(pat="命中占融202098联合规则强拒", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j34 = df_j34.rename(columns={"order_id": "命中占融202098联合规则强拒"})
        df_dj_last21 = pd.merge(df_dj_last20, df_j34, on=model, how='outer')

        # 命中占融202100联合规则强拒
        df_j35 = df_j[df_j["拒绝理由"].str.contains(pat="命中占融202100联合规则强拒", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j35 = df_j35.rename(columns={"order_id": "命中占融202100联合规则强拒"})
        df_dj_last22 = pd.merge(df_dj_last21, df_j35, on=model, how='outer')

        # 命中策略strategy_b004强拒
        df_j36 = df_j[df_j["拒绝理由"].str.contains(pat="命中策略strategy_b004强拒", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j36 = df_j36.rename(columns={"order_id": "命中策略strategy_b004强拒"})
        df_dj_last23 = pd.merge(df_dj_last22, df_j36, on=model, how='outer')

        # 以下五条为拒量独有

        # 命中250513规则1强拒
        df_j37 = df_j[df_j["拒绝理由"].str.contains(pat="命中250513规则1强拒", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j37 = df_j37.rename(columns={"order_id": "命中250513规则1强拒"})
        df_dj_last24 = pd.merge(df_dj_last23, df_j37, on=model, how='outer')

        # 命中250513规则2强拒
        df_j38 = df_j[df_j["拒绝理由"].str.contains(pat="命中250513规则2强拒", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j38 = df_j38.rename(columns={"order_id": "命中250513规则2强拒"})
        df_dj_last25 = pd.merge(df_dj_last24, df_j38, on=model, how='outer')

        # 命中250513规则3强拒
        df_j39 = df_j[df_j["拒绝理由"].str.contains(pat="命中250513规则3强拒", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j39 = df_j39.rename(columns={"order_id": "命中250513规则3强拒"})
        df_dj_last26 = pd.merge(df_dj_last25, df_j39, on=model, how='outer')

        # 命中250513规则4强拒
        df_j40 = df_j[df_j["拒绝理由"].str.contains(pat="命中250513规则4强拒", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j40 = df_j40.rename(columns={"order_id": "命中250513规则4强拒"})
        df_dj_last27 = pd.merge(df_dj_last26, df_j40, on=model, how='outer')

        # 命中250513规则5强拒
        df_j41 = df_j[df_j["拒绝理由"].str.contains(pat="命中250513规则5强拒", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j41 = df_j41.rename(columns={"order_id": "命中250513规则5强拒"})
        df_dj_last28 = pd.merge(df_dj_last27, df_j41, on=model, how='outer')


        # # 以下为京享租独有

        # 命中策略JDB002_212强拒
        df_j42 = df_j[df_j["拒绝理由"].str.contains(pat="命中策略JDB002_212强拒", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j42 = df_j42.rename(columns={"order_id": "命中策略JDB002_212强拒"})
        df_dj_last29 = pd.merge(df_dj_last28, df_j42, on=model, how='outer')

        # 命中JDB002_202100联合强拒
        df_j43 = df_j[df_j["拒绝理由"].str.contains(pat="命中JDB002_202100联合强拒", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j43 = df_j43.rename(columns={"order_id": "命中JDB002_202100联合强拒"})
        df_dj_last30 = pd.merge(df_dj_last29, df_j43, on=model, how='outer')

        # 命中JDB002_202098强拒
        df_j44 = df_j[df_j["拒绝理由"].str.contains(pat="命中JDB002_202098强拒", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j44 = df_j44.rename(columns={"order_id": "命中JDB002_202098强拒"})
        df_dj_last31 = pd.merge(df_dj_last30, df_j44, on=model, how='outer')

        # 命中JDB002_201048强拒
        df_j45 = df_j[df_j["拒绝理由"].str.contains(pat="命中JDB002_201048强拒", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j45 = df_j45.rename(columns={"order_id": "命中JDB002_201048强拒"})
        df_dj_last32 = pd.merge(df_dj_last31, df_j45, on=model, how='outer')

        # 命中京享租信用等级低于D
        df_j46 = df_j[df_j["total_describes"].str.contains(pat="命中京享租信用等级低于D", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j46 = df_j46.rename(columns={"order_id": "命中京享租信用等级低于D"})
        df_dj_last33 = pd.merge(df_dj_last32, df_j46, on=model, how='outer')

        # 命中JDB003_rule_1强拒
        df_j47 = df_j[df_j["拒绝理由"].str.contains(pat="命中JDB003_rule_1强拒", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j47 = df_j47.rename(columns={"order_id": "命中JDB003_rule_1强拒"})
        df_dj_last34 = pd.merge(df_dj_last33, df_j47, on=model, how='outer')

        # 命中JDB003_rule_2强拒
        df_j48 = df_j[df_j["拒绝理由"].str.contains(pat="命中JDB003_rule_2强拒", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j48 = df_j48.rename(columns={"order_id": "命中JDB003_rule_2强拒"})
        df_dj_last35 = pd.merge(df_dj_last34, df_j48, on=model, how='outer')

        # 命中JDB003_rule_4强拒
        df_j49 = df_j[df_j["拒绝理由"].str.contains(pat="命中JDB003_rule_4强拒", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j49 = df_j49.rename(columns={"order_id": "命中JDB003_rule_4强拒"})
        df_dj_last36 = pd.merge(df_dj_last35, df_j49, on=model, how='outer')

        df_j50 = df_j[df_j["拒绝理由"].str.contains(pat="命中JDB003_fico强拒", regex=False) == True].groupby(model).agg({"order_id": np.size}).reset_index()
        df_j50 = df_j50.rename(columns={"order_id": "命中JDB003_fico强拒"})
        df_dj_last37 = pd.merge(df_dj_last36, df_j50, on=model, how='outer')

        return df_dj_last37


class Math_Calculation:
    # 定义 PSI 计算函数
    def calculate_psi(self, expected_pct, actual_pct):
        psi_contributions = []

        for exp, act in zip(expected_pct, actual_pct):
            if exp == 0 or act == 0:
                psi_contributions.append(0)  # 避免除以零或对数未定义的情况
            else:
                psi_contributions.append((act - exp) * log(act / exp))

        return sum(psi_contributions)