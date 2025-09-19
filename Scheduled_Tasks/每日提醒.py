from calendar import month
import gc
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from apscheduler.schedulers.background import BackgroundScheduler

# import seaborn as sns
#sns.set(style="darkgrid")
plt.rcParams["font.sans-serif"]=["SimHei"]
plt.rcParams["axes.unicode_minus"]=False

import warnings
warnings.filterwarnings("ignore")

import sys
import pymysql
from sqlalchemy import create_engine
import json
from datetime import timedelta ,datetime, timezone
import time
from dateutil.relativedelta import relativedelta
import sys
from pathlib import Path

# 将项目根目录添加到 sys.path
sys.path.append(str(Path(__file__).parent.parent))  # 依实际路径调整
from Class_Model.All_Class import All_Model, Week_Model, Data_Clean

class Tips:
    def __init__(self):
        pd.set_option('display.max_columns', None)
        self.all_models = All_Model()
        self.week_models = Week_Model()
        self.clean = Data_Clean()


    def select_date(self):
        sql1 = '''
        SELECT tt.order_id, 
        tt.order_number '订单号', tt.true_name '姓名', tt.user_mobile '手机号', tt.product_name '产品名称', tt.create_time '下单日期', tt.refund_date '预计还款日期', tt.reality_refund_date '实际还款日期', 
        tt.money '应付金额', (tt.part_payment+tt.sesame_promo_money_pay) '实付金额',tt.sesame_promo_money, tt.sesame_promo_money_pay,tt.min_date, tt.max_date, tt.diff_date
        FROM (
        SELECT  t.order_id, t.order_number,t.true_name,t.user_mobile,t.product_name,t.create_time, t.refund_date,t.reality_refund_date,t.money
        ,t.part_payment,t.min_date,
        case when max_date>=DATE_ADD(CURRENT_DATE, INTERVAL -1 DAY) then CURRENT_DATE else t.max_date END max_date,
        case when t.min_date=t.max_date then DATEDIFF(CURRENT_DATE, t.min_date) 
                    when max_date>CURRENT_DATE then DATEDIFF(CURRENT_DATE, t.min_date) 
                    else DATEDIFF(t.max_date, t.min_date) END diff_date
        ,t.sesame_promo_money, t.sesame_promo_money_pay
        FROM (
        SELECT -- 查询总体订单和去重以及剔除商家 
        om.id as order_id, om.order_number,tmu.true_name,om.user_mobile,tod.product_name,om.create_time,ymos.refund_date,ymos.reality_refund_date
        ,ymos.money,ymos.part_payment,ymos.sesame_promo_money, ymos.sesame_promo_money_pay
        ,min(ymos.refund_date)over(partition by om.order_number) min_date
        ,max(ymos.refund_date)over(partition by om.order_number) max_date
        FROM db_digua_business.t_postlease_receivables_monitoring   tprm 
        LEFT JOIN db_digua_business.t_order om on tprm.order_id = om.id 
        LEFT JOIN db_digua_business.t_merchant tmer on tmer.id = tprm.merchant_id
        LEFT JOIN db_rent.ya_merchant_order_stages ymos on om.id=ymos.order_id
        LEFT JOIN db_digua_business.t_order_details tod on tod.order_id=om.id 
        LEFT JOIN db_digua_business.t_member_user tmu on tmu.mobile=om.user_mobile
        left join db_digua_business.t_platform_activity pa on om.activity_id = pa.id
        WHERE om.`status` in (4) 
        AND tmer.shop_type !=2  -- 剔除企业 1、自营店铺；2、入驻店铺；3、代运营店铺'
        AND tprm.merchant_id  not in (15,99)  -- 剔除CPS渠道合作 汇客好租
        AND tprm.model_number not like '%探路者%'
        AND pa.type!=4
        AND (ymos.part_payment+ymos.sesame_promo_money_pay)<(ymos.money+ymos.sesame_promo_money)
        and ymos.reality_refund_date is null
        and om.has_actual!=1
        ) t
        ) tt
        WHERE diff_date<=0 and refund_date>=CURRENT_DATE and refund_date<=DATE_ADD(CURRENT_DATE, INTERVAL +0 DAY);
        '''
        df_order = self.clean.query(sql1)
        sql_name = '''
                SELECT tuvor.order_id, tu.nick_name 分配人, tuvor.update_time 
                FROM db_digua_business.t_user_verify_order_record tuvor
                left join db_digua_business.t_user tu on tuvor.user_id = tu.id 
                where tuvor.del_flag = 0 ORDER BY tuvor.update_time
                '''
        df_name = self.clean.query(sql_name)
        return df_order, df_name

    def run(self):
        df, df_name = self.select_date()
        df_xs = df[['order_id', '订单号', '姓名', '手机号', '产品名称', '下单日期', '预计还款日期', '应付金额']].drop_duplicates()
        # 碰到特殊情况可将month变为31day，例如9月底应跑出8月27和28的数据
        df_xs = df_xs[pd.to_datetime(df_xs.下单日期.dt.date) == pd.to_datetime(df_xs.预计还款日期)-pd.DateOffset(months=1)-timedelta(days=3)]
        # 获取订单的最后一位分配人并关联分配数据
        df_name_new = df_name.sort_values('update_time', ascending=False).groupby('order_id').head(1)
        df_xs_merge = df_xs.merge(df_name_new[['order_id', '分配人']], on='order_id', how='left')
        # 创建分配人映射关系
        name_dict = {
            '小张': '李巧玲',
            '小周': '李巧凤',
            '小南': '刘三妹',
            '小何': '何静',
            '小谢': '谢金凤',
            '小咪': '杨健',
            '小慧': '林思慧',
            '小滢': '胡彩滢',
            '小星': '廖丽敏',
            '小兰': '黄兰娟',
            '小晚': '周莹',
            '小芳': '罗芳',
        }
        name_list = ['罗文龙', '何静', '刘三妹', '杨健', '林思慧', '胡彩滢', '周汉鸿', '廖丽敏', '黄兰娟', '周莹', '邹巧巧', '冯二祥','罗芳', '魏朵','周念慈',]
        # 匹配映射字典
        df_xs_merge.loc[:, 'name'] = df_xs_merge.分配人.apply(lambda x: name_dict[x] if str(x).startswith('小') and str(x) in name_dict.keys() else x)
        df_xs_merge = df_xs_merge[df_xs_merge.name.isin(name_list)]
        return df_xs_merge

    def my_job(self, hour, minute, path1):

        now_date = datetime.now().strftime('%Y-%m-%d')
        print(f'执行定时任务：现在是{now_date}的{hour}:{minute}')
        df_xs = self.run()
        with pd.ExcelWriter(path1 + f'到期订单_信审2_{now_date}.xlsx', engine='openpyxl') as writer:
            df_xs.to_excel(writer, sheet_name=now_date, index=False)

        del df_xs
        gc.collect()
        print("回收内存执行完毕！\n")

if __name__ == '__main__':
    T = Tips()
    # s = T.run()
    # print(s)
    hour = 15
    minute = 1
    # path1 = 'F:/需求/到期订单/'
    path1 = r'\\digua\迪瓜租机\13.每日扣款提醒/'
    # 实时手动跑
    # T.my_job(hour, minute, path1)
    print('正在创建定时任务...')
    scheduler = BackgroundScheduler()
    # 每天15点01分开始执行
    job = scheduler.add_job(T.my_job, 'cron', hour=hour, minute=minute, args=[hour, minute, path1])
    print('定时任务创建完毕...\n正在执行定时任务my_job...')
    print(scheduler.get_jobs())
    scheduler.start()
    print('定时任务执行完毕,请查看数据...')
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
        gc.collect()