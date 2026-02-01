【已实现】

实现用“每仓库 watermark（水位）+ overlap 回看”做增量，不依赖固定周期——你隔 1 天、3 天甚至一周才跑一次，
从上次成功采集的位置继续往后拉。

关键点：
1，watermark 存什么：建议存该仓库已入库的最新 committed_at（或最新 commit_sha + committed_at）。
2，本次增量怎么拉：since = watermark - overlap（例如回看 1 天），调用 ListCommits(since=since)。
3，怎么保证不重复：silver 层按 (repo_id, sha) 去重/upsert，重复拉到的 overlap 部分会被过滤掉。
4，怎么补漏：如果中间隔了很多天没跑，API 会把这段时间内所有 commits 都返回；你照常入库即可。
5，失败场景：某 repo 拉取失败（403/404/网络）就不更新该 repo 的 watermark，下次再跑会继续从旧水位重试，不会“跳过缺口”。