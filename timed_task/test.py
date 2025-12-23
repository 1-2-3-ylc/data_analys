import numpy as np
from rank_bm25 import BM25Okapi
from sentence_transformers import SentenceTransformer, CrossEncoder
from sklearn.metrics.pairwise import cosine_similarity
import jieba

# ====================== 1. 初始化模型和模拟数据 ======================
# 1.1 初始化向量模型（用于向量召回）
vector_model = SentenceTransformer('all-MiniLM-L6-v2')
# 1.2 初始化重排模型（Cross-Encoder）
reranker_model = CrossEncoder('cross-encoder/ms-marco-MiniLM-L-6-v2')
# 1.3 模拟电商商品文档库（RAG语料库）
corpus = [
    "iPhone17Promax屏幕尺寸6.7英寸，电池容量4800mAh，搭载A19 Pro处理器，支持45W快充",
    "iPhone17屏幕尺寸6.1英寸，电池容量4200mAh，搭载A19处理器，支持20W快充",
    "iPhone17Promax起售价8999元，主要面向高端用户，拍照配备4800万主摄+1200万长焦",
    "iPhone17起售价6999元，适合追求均衡体验的用户，单4800万主摄",
    "iPhone17Pro屏幕6.3英寸，电池4500mAh，A19 Pro处理器，30W快充",
    "iPhone16维修指南：电池更换价格299元，屏幕更换899元",
    "苹果快充充电器选购：45W快充头适配iPhone17Promax，20W适配iPhone17"
]
# 1.4 模拟用户查询（电商客服真实场景）
user_query = "iPhone17Promax的电池续航和快充参数"

# ====================== 2. 多路召回模块 ======================
class MultiPathRetrieval:
    def __init__(self, corpus, vector_model):
        self.corpus = corpus
        self.vector_model = vector_model
        # 预处理BM25所需的分词文本
        self.corpus_tokenized = [list(jieba.cut(doc)) for doc in corpus]
        self.bm25 = BM25Okapi(self.corpus_tokenized)
        # 预编码语料库向量（工程中可缓存，避免重复编码）
        self.corpus_vectors = vector_model.encode(corpus)

    def bm25_retrieval(self, query, top_k=20):
        """BM25关键词召回"""
        query_tokenized = list(jieba.cut(query))
        scores = self.bm25.get_scores(query_tokenized)
        # 按分数排序，取top_k
        top_indices = np.argsort(scores)[::-1][:top_k]
        return [(self.corpus[i], scores[i], i) for i in top_indices]

    def vector_retrieval(self, query, top_k=20):
        """向量语义召回"""
        query_vector = self.vector_model.encode([query])
        similarities = cosine_similarity(query_vector, self.corpus_vectors)[0]
        # 按相似度排序，取top_k
        top_indices = np.argsort(similarities)[::-1][:top_k]
        return [(self.corpus[i], similarities[i], i) for i in top_indices]

    def merge_retrieval_results(self, query, top_k_per_path=20):
        """合并多路召回结果，去重"""
        # 执行两路召回
        bm25_results = self.bm25_retrieval(query, top_k_per_path)
        vector_results = self.vector_retrieval(query, top_k_per_path)
        
        # 去重：用文档索引去重，保留所有分数
        merged_dict = {}
        # 先加BM25结果
        for doc, score, idx in bm25_results:
            merged_dict[idx] = {
                "doc": doc,
                "bm25_score": score,
                "vector_score": 0.0,
                "bm25_rank": list([x[2] for x in bm25_results]).index(idx) + 1  # 排名从1开始
            }
        # 再加向量结果，补充分数和排名
        for doc, score, idx in vector_results:
            vector_rank = list([x[2] for x in vector_results]).index(idx) + 1
            if idx in merged_dict:
                merged_dict[idx]["vector_score"] = score
                merged_dict[idx]["vector_rank"] = vector_rank
            else:
                merged_dict[idx] = {
                    "doc": doc,
                    "bm25_score": 0.0,
                    "vector_score": score,
                    "bm25_rank": np.inf,  # 无排名设为无穷大
                    "vector_rank": vector_rank
                }
        
        # 转换为列表返回
        merged_results = list(merged_dict.values())
        return merged_results

# ====================== 3. 重排模块 ======================
class Reranker:
    def __init__(self, reranker_model):
        self.reranker_model = reranker_model

    def rrf_fusion(self, merged_results, k=60):
        """RRF倒数排名融合，计算综合得分"""
        for res in merged_results:
            # 计算单路RRF分数，无穷大排名得0分
            bm25_rrf = 1 / (k + res["bm25_rank"]) if res["bm25_rank"] != np.inf else 0.0
            vector_rrf = 1 / (k + res["vector_rank"]) if res["vector_rank"] != np.inf else 0.0
            # 综合RRF分数
            res["rrf_score"] = bm25_rrf + vector_rrf
        # 按RRF分数排序
        sorted_by_rrf = sorted(merged_results, key=lambda x: x["rrf_score"], reverse=True)
        return sorted_by_rrf

    def cross_encoder_rerank(self, query, rrf_sorted_results, top_k=10):
        """Cross-Encoder重排，对RRF结果二次打分"""
        # 取RRF排序后的top_k条做重排（减少计算量）
        candidate_docs = [res["doc"] for res in rrf_sorted_results[:top_k]]
        # 构造"查询-文档"对
        query_doc_pairs = [[query, doc] for doc in candidate_docs]
        # 模型打分（分数越高，相关性越强）
        rerank_scores = self.reranker_model.predict(query_doc_pairs)
        
        # 合并分数并排序
        rerank_results = []
        for i, res in enumerate(rrf_sorted_results[:top_k]):
            res["cross_score"] = rerank_scores[i]
            rerank_results.append(res)
        # 按Cross-Encoder分数排序
        sorted_by_cross = sorted(rerank_results, key=lambda x: x["cross_score"], reverse=True)
        return sorted_by_cross

# ====================== 4. 主流程执行 ======================
if __name__ == "__main__":
    # 初始化多路召回器
    retriever = MultiPathRetrieval(corpus, vector_model)
    # 执行多路召回并合并结果
    merged_results = retriever.merge_retrieval_results(user_query, top_k_per_path=5)  # 示例取top5，工程可设20
    print("===== 多路召回合并结果（去重后） =====")
    for i, res in enumerate(merged_results):
        print(f"{i+1}. 文档：{res['doc'][:50]}... | BM25排名：{res['bm25_rank']} | 向量排名：{res['vector_rank']}")

    # 初始化重排器
    reranker = Reranker(reranker_model)
    # 第一步重排：RRF融合
    rrf_sorted = reranker.rrf_fusion(merged_results, k=60)
    print("\n===== RRF重排结果 =====")
    for i, res in enumerate(rrf_sorted):
        print(f"{i+1}. 文档：{res['doc'][:50]}... | RRF分数：{res['rrf_score']:.4f}")

    # 第二步重排：Cross-Encoder二次打分
    cross_sorted = reranker.cross_encoder_rerank(user_query, rrf_sorted, top_k=5)
    print("\n===== Cross-Encoder最终重排结果 =====")
    for i, res in enumerate(cross_sorted):
        print(f"{i+1}. 文档：{res['doc'][:50]}... | Cross分数：{res['cross_score']:.4f}")

    # 最终输出Top3文档（传给LLM）
    final_top3 = [res["doc"] for res in cross_sorted[:3]]
    print("\n===== RAG最终传给LLM的Top3文档 =====")
    for i, doc in enumerate(final_top3):
        print(f"{i+1}. {doc}")
        
        
        
        