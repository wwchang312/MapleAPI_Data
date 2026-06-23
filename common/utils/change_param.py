class ChangeParma:

    def __init__(self,param:list[str],param_name:str):
        self.param = param
        self.param_name = param_name

    def mapping_array_alias(self):

        if self.param_name == 'character_info_dataset':
            # 캐릭터 정보 데이터 셋의 alias
            character_info_dataset = {
                "기본정보": "character/basic",
                "인기도": "character/popularity",
                "종합_능력치": "character/stat",
                "하이퍼스탯": "character/hyper-stat",
                "성향": "character/propensity",
                "어빌리티": "character/ability",
                "장착_장비": "character/item-equipment",
                "장착_캐시_장비": "character/cashitem-equipment",
                "장착_심볼": "character/symbol-equipment",
                "적용_세트효과": "character/set-effect",
                "장착_뷰티아이템": "character/beauty-equipment",
                "장착_안드로이드": "character/android-equipment",
                "장착_펫": "character/pet-equipment",
                "스킬": "character/skill",
                "링크스킬": "character/link-skill",
                "V매트릭스": "character/vmatrix",
                "HEXA코어": "character/hexamatrix",
                "HEXA매트릭스_HEXA스탯": "character/hexamatrix-stat",
                "무릉도장": "character/dojang",
                "기타_능력치_영향_요소": "character/other-stat",
                "링_익스체인지_스킬_등록_장비": "character/ring-exchange-skill-equipment",
                "예비_특수반지_장착_정보": "character/ring-reserve-skill-equipment",
            }
            if not self.param:
                param_lst=[i for i in character_info_dataset.values()]

                return param_lst

            else:
                param_lst=[i for i in self.param]

                return param_lst
        return None