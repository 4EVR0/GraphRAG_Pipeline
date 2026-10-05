# 성분 × 효능 좁은 검색 설정 (#49)

`run_bronze --mode narrow`가 읽는 설정입니다. 성분별 상위 N건을 자르는 대신,
(성분, 효능) 조합마다 좁게 검색해 결과를 전부 가져옵니다.

## 파일

| 파일 | 내용 |
|---|---|
| `tier1_pairs.csv` | 검색할 (성분, 효능) 조합 |
| `effect_terms.csv` | 효능별 검색 용어와 피부 특이 여부 |
| `ingredient_rules.csv` | 몸속 물질·일반 원료 규칙 |

### tier1_pairs.csv
- 운영 그래프 읽기 결과(2026-10-03)로 만든 1순위 목록입니다. 금지 성분은 제외했습니다.
  - `selection_reasons`
    - `mfds`: 식약처 고시 기능성 원료
    - `exposed`: 추천 서버의 고민별 성분 후보 상위 20위 안
    - `products100`: 함유 제품 100개 이상
- 효능은 그래프의 효능 정보(`graph`)와 식약처 고시 기능에서 온 효능(`mfds`)입니다.
  - 식약처 고시 기능은 미백 → DEPIGMENTING·BRIGHTENING, 주름 → ANTI_AGING, 여드름 → COMEDOLYTIC·SEBUM_REGULATION·KERATOLYTIC·ANTI_INFLAMMATORY, 자외선 차단 → PHOTOPROTECTIVE로 바꿉니다.
  - 결과 효능 BLEMISH_CARE는 검색 대상에서 뺍니다.
- 성분 332개 중 효능 정보가 있는 269개, 769개 조합입니다.
- 목록을 바꿀 때는 기준일과 규칙을 이 문서에 함께 적습니다.

### effect_terms.csv
- `skin_specific=true`인 효능(여드름·색소·장벽·피지·광보호·미백)은 그대로 검색합니다.
- `false`인 효능에는 피부 맥락 조건(skin, topical, cutaneous, dermal, facial, cosmetic, dermatolog* 등)을 붙입니다.

### ingredient_rules.csv
- `exclude`: 검색하지 않습니다(용매 등 효능 근거 대상이 아닌 원료).
- `require_topical`: 외용 조건(topical, cosmetic*, cream, lotion, ointment, skin care 등)을 필수로 붙입니다.
  - 혈청·전기영동과 겹치는 serum·gel은 외용 조건에서 뺍니다.

## 검색어 규칙
- 성분: INCI명 + `config/target_ingredients.csv`의 검색명·동의어
  - 대문자 2~5자 약어 동의어(BHA 등)는 다른 물질과 겹치므로 뺍니다. INCI명 자체는 항상 씁니다.
- 성분명과 겹치는 효능 용어는 그 성분의 검색어에서 뺍니다.
  - 예: MELANIN의 DEPIGMENTING 검색에서 "melanin"을 뺍니다.
  - 효능 용어가 하나도 남지 않으면 그 조합은 검색하지 않고 `self_match`로 기록합니다.
- 공통 필터: `hasabstract AND english[lang]`

## 건수 규칙
- 조합당 결과가 `NARROW_FULL_FETCH_MAX`(300) 이하면 전부 가져옵니다.
- 넘으면 `humans[mh] AND (clinical trial | RCT | review | systematic review | meta-analysis)`로 좁힌 뒤 전부 가져옵니다.
- 좁힌 뒤에도 `NARROW_PAIR_CAP`(5000)을 넘으면 그만큼만 가져옵니다. 이런 조합은 `capped`로 기록하고 경고 로그를 남깁니다.
- 중복은 PMID 기준으로 제거합니다. `--skip-pmids-from`에 준 PMID는 다시 받지 않습니다.
