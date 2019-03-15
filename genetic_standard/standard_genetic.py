

import time
import pytz
import datetime
import mongo_genetic
import copy
import time
import common_genetic as common

tz = pytz.timezone('Asia/Shanghai')


class StandardGenetic:

    def __init__(self):
        self.date = None
        self.ads = list()
        self.ads_other = list()

    def get_today(self):
        self.date = datetime.datetime.fromtimestamp(int(time.time()), pytz.timezone('Asia/Shanghai'))

    def loading_data(self):
        # self.ads = mongo_genetic.find_evaluation_today((self.date + datetime.timedelta(days=-2)).strftime('%Y-%m-%d'))
        self.ads_other = mongo_genetic.find_evaluation_before((self.date + datetime.timedelta(days=-20)).strftime('%Y-%m-%d'),
                                                              (self.date + datetime.timedelta(days=-0)).strftime('%Y-%m-%d'))

    def compose_child(self, father, mother):
        if mother['type'] == 'behaviors':
            father['pt']['adset_spec']['targeting']['behaviors'] = copy.deepcopy(mother['pt']['adset_spec']['targeting']['behaviors'])
        elif mother['type'] == 'interests':
            father['pt']['adset_spec']['targeting']['interests'] = copy.deepcopy(mother['pt']['adset_spec']['targeting']['interests'])

        return copy.deepcopy(father)

    def data_workhouse(self):
        pts_pool = list()
        # for father in self.ads:
        #     for mother in self.ads_other:
        #         if father['type'] != mother['type'] and (father['type'] == 'seed' or mother['type'] == 'seed'):
        #             if father['type'] == 'seed':
        #                 tmp_pt = self.compose_child(copy.deepcopy(father), copy.deepcopy(mother))
        #                 pts_pool.append(common.modify_name(copy.deepcopy(tmp_pt)))
        #             else:
        #                 tmp_pt = self.compose_child(copy.deepcopy(mother), copy.deepcopy(father))
        #                 pts_pool.append(common.modify_name(copy.deepcopy(tmp_pt)))
        combo_adids = common.read_combo_adids()
        for findex in range(len(self.ads_other)-1):
            for mindex in range(findex+1, len(self.ads_other)):
                father = self.ads_other[findex]
                mother = self.ads_other[mindex]
                if father['type'] != mother['type'] and (father['type'] == 'seed' or mother['type'] == 'seed'):
                    if father['type'] == 'seed' and father['ad_id']+'&'+mother['ad_id'] not in combo_adids:
                        tmp_pt = self.compose_child(copy.deepcopy(father), copy.deepcopy(mother))
                        pts_pool.append(common.modify_name(copy.deepcopy(tmp_pt)))
                        combo_adids.append(father['ad_id']+'&'+mother['ad_id'])
                    elif mother['ad_id']+'&'+father['ad_id'] not in combo_adids:
                        tmp_pt = self.compose_child(copy.deepcopy(mother), copy.deepcopy(father))
                        pts_pool.append(common.modify_name(copy.deepcopy(tmp_pt)))
                        combo_adids.append(mother['ad_id']+'&'+father['ad_id'])

        common.save_combo_adids(combo_adids)
        print(pts_pool)
        print(len(pts_pool))
        # mongo_genetic.insert_baits(pts_pool)

    def main(self):
        t1 = time.time()
        self.get_today()
        self.loading_data()
        print(time.time()-t1)
        self.data_workhouse()
        print(time.time()-t1)
        pass


if __name__ == '__main__':
    sg = StandardGenetic()
    sg.main()
