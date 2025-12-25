import pandas as pd
from rapidfuzz import process, fuzz
from datetime import datetime
import numpy as np
import json
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser

# 引入我们的 LLM 工厂
from app.services.llm_factory import get_llm

# 尝试导入向量库
try:
    from sentence_transformers import SentenceTransformer, util
    HAS_VECTOR_MODEL = True
except ImportError:
    HAS_VECTOR_MODEL = False
    print("⚠️ 未检测到 sentence-transformers，将仅使用字符串匹配模式。")

class AuditLogger:
    """审计日志记录器"""
    def __init__(self):
        self.logs = []
        self.excluded_data = {}

    def info(self, step_name: str, description: str, affected_rows: int = 0):
        entry = {
            "Timestamp": datetime.now().strftime("%H:%M:%S"),
            "Step": step_name,
            "Description": description,
            "Affected_Rows": affected_rows,
            "Type": "Operation"
        }
        self.logs.append(entry)
        # 控制台依然打印简略版，防止刷屏
        print(f"📝 [Audit] {step_name}: {description.splitlines()[0]}... (Rows: {affected_rows})")

    def log_exclusion(self, step_name: str, description: str, excluded_df: pd.DataFrame):
        rows = len(excluded_df)
        entry = {
            "Timestamp": datetime.now().strftime("%H:%M:%S"),
            "Step": step_name,
            "Description": description,
            "Affected_Rows": rows,
            "Type": "Exclusion"
        }
        self.logs.append(entry)
        if rows > 0:
            safe_name = f"{step_name}_{len(self.excluded_data)}"
            self.excluded_data[safe_name] = excluded_df.head(100)
            print(f"🗑️ [Audit-Exclusion] {step_name}: Removed {rows} rows.")

    def get_log_df(self):
        return pd.DataFrame(self.logs)

class VectorMatcher:
    """语义向量匹配器 (负责召回 Candidates)"""
    _instance = None
    _model = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(VectorMatcher, cls).__new__(cls)
            if HAS_VECTOR_MODEL:
                print("⏳ [System] 正在加载语义向量模型 (paraphrase-multilingual)...")
                cls._model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
                print("✅ 模型加载完毕")
        return cls._instance

    def get_candidates(self, source_word: str, target_candidates: list, top_k=5):
        """返回最相似的 Top K 个候选项"""
        if not self._model: return []
        
        source_emb = self._model.encode(source_word, convert_to_tensor=True)
        target_embs = self._model.encode(target_candidates, convert_to_tensor=True)
        
        cosine_scores = util.cos_sim(source_emb, target_embs)[0]
        
        # 获取 Top K
        k = min(top_k, len(target_candidates))
        top_results = np.argpartition(-cosine_scores.cpu().numpy(), range(k))[:k]
        
        candidates = []
        for idx in top_results:
            score = cosine_scores[idx].item()
            # 💡 降级阈值：只要有一点点相关(0.1)就召回，交给 LLM 去判断
            if score > 0.1:
                candidates.append((target_candidates[idx], score))
        
        candidates.sort(key=lambda x: x[1], reverse=True)
        return candidates

class LLMJudge:
    """LLM 裁判：利用大模型的世界知识做最终决定"""
    def __init__(self):
        self.llm = get_llm(temperature=0)
        
    def judge(self, source: str, candidates: list) -> str:
        if not candidates: return None
        
        cand_names = [c[0] if isinstance(c, tuple) or isinstance(c, list) else c for c in candidates]
        
        prompt = ChatPromptTemplate.from_messages([
            ("system", """
            你是一个实体对齐专家。
            任务：判断左边的【源实体】是否对应右边候选列表中的某个【标准实体】。
            
            规则：
            1. 利用你的世界知识（包括中英文名、简称、别名、拼音、收购关系）。
               例如: "ByteDance" == "字节跳动", "Meituan" == "美团点评", "JD" == "京东"。
            2. 如果找到了确定的匹配，只返回该标准实体的名称。
            3. 如果所有候选都不匹配，或者非常不确定，返回 "None"。
            4. 只返回名称字符串，不要有任何标点或解释。
            """),
            ("human", "源实体: '{source}'\n候选列表: {candidates}")
        ])
        
        try:
            chain = prompt | self.llm | StrOutputParser()
            result = chain.invoke({"source": source, "candidates": str(cand_names)})
            result = result.strip().replace("'", "").replace('"', "")
            
            if result == "None" or result not in cand_names:
                return None
            return result
        except:
            return None

def smart_merge(left_df: pd.DataFrame, right_df: pd.DataFrame, 
                left_on: str, right_on: str, 
                logger: AuditLogger = None) -> pd.DataFrame:
    """
    智能三级匹配：Fuzz -> Adaptive LLM
    """
    left_keys = left_df[left_on].astype(str).unique()
    right_keys = right_df[right_on].astype(str).unique()
    
    mapping = {}
    matched_log = []
    
    vector_matcher = VectorMatcher() if HAS_VECTOR_MODEL else None
    llm_judge = LLMJudge()
    
    print(f"🔍 [SmartMerge] 开始智能匹配 (Left: {len(left_keys)}, Right: {len(right_keys)})")
    
    # 策略选择
    use_full_llm_match = len(right_keys) <= 50
    if use_full_llm_match:
        print("   🚀 [Strategy] 目标数据量较小，启用 LLM 全量精准匹配模式")
    
    for lk in left_keys:
        final_target = None
        method = "None"
        
        # Level 1: Fuzz
        match = process.extractOne(lk, right_keys, scorer=fuzz.WRatio)
        if match:
            target, score, _ = match
            if score >= 90:
                final_target = target
                method = f"Fuzz({int(score)})"
        
        # Level 2: LLM
        if not final_target:
            candidates = []
            if use_full_llm_match:
                candidates = list(right_keys)
            elif vector_matcher:
                candidates = vector_matcher.get_candidates(lk, right_keys, top_k=5)
            
            if candidates:
                llm_choice = llm_judge.judge(lk, candidates)
                if llm_choice:
                    final_target = llm_choice
                    source_type = "FullList" if use_full_llm_match else f"VectorTop{len(candidates)}"
                    method = f"LLM({source_type})"
        
        # 记录
        if final_target:
            mapping[lk] = final_target
            if lk != final_target:
                matched_log.append(f"[{method}] '{lk}' -> '{final_target}'")
        else:
            mapping[lk] = None
            
    # 执行映射
    temp_col = f"_smart_join_{right_on}"
    left_df_mapped = left_df.copy()
    left_df_mapped[temp_col] = left_df_mapped[left_on].astype(str).map(mapping)
    
    # ✅ 修复点：将详细日志写入 Description
    if logger:
        success_count = len([x for x in mapping.values() if x is not None])
        desc = f"智能匹配: 输入 {len(left_keys)} 个实体，成功匹配 {success_count} 个。"
        
        if matched_log:
            # 将匹配细节追加到描述中
            detail_str = "\n".join(matched_log)
            desc += f"\n\n--- 匹配详情 ({len(matched_log)} 条) ---\n{detail_str}"
            
        logger.info("Smart Merge", desc, affected_rows=len(matched_log))
        
        if matched_log:
            print(f"   ✨ 匹配高光时刻:\n   " + "\n   ".join(matched_log[:5]) + "...")

    # 执行 Merge
    merged = pd.merge(left_df_mapped, right_df, left_on=temp_col, right_on=right_on, how='left')
    
    if temp_col in merged.columns:
        del merged[temp_col]
        
    return merged