import { useReducer, useEffect, useRef } from "react";
import location from "@/services/location";
import recordEvent from "@/services/recordEvent";
import { ExecutionStatus } from "@/services/query-result";
import notifications from "@/services/notifications";
import useImmutableCallback from "@/lib/hooks/useImmutableCallback";

function getMaxAge() {
  const { maxAge } = location.search;
  return maxAge !== undefined ? maxAge : -1;
}

const reducer = (prevState, updatedProperty) => ({
  ...prevState,
  ...updatedProperty,
});

// This is currently specific to a Query page, we can refactor
// it slightly to make it suitable for dashboard widgets instead of the other solution it
// has in there.
export default function useQueryExecute(query) {
  // 初期化
  const [dryRunState, setDryRunState] = useReducer(reducer, {
    dryRunResult: null,
    isDryRunning: false,
    loadedInitialDryRunResults: false,
    dryRunStatus: null,
    isDryRunCancelling: false,
    dryRunCancelCallback: null,
    dryRunError: null,
  });
  // 初期化
  const dryRunResultInExecution = useRef(null);
  // Clear executing queryResult when component is unmounted to avoid errors
    // 初期化
  useEffect(() => {
    return () => {
      dryRunResultInExecution.current = null;
    };
  }, []);

  // クエリ実行処理を定義する
  const dryRunQuery = useImmutableCallback(() => {
    // クエリの実行結果を取得する手段をここで定義する
    let newDryRunResult;
    // TODO: impl getDryRunResult
    newDryRunResult = query.getDryRunResult();

    recordEvent("dry_run", "query", query.id);
    notifications.getPermissions();

    // 状態更新
    // 今見てる実行中のクエリはクエリの実行結果の取得方法を設定したもの
    dryRunResultInExecution.current = newDryRunResult;

    // 情報更新
    setDryRunState({
      dryRunUpdatedAt: newDryRunResult.getUpdatedAt(),
      // TODO: impl getDryRunStatus
      dryRunStatus: newDryRunResult.getDryRunStatus(),
      isDryRunning: true,
      dryRunCancelCallback: () => {
        recordEvent("cancel_execute", "query", query.id);
        setDryRunState({ isCancelling: true });
        // TODO: impl cancelDryRun
        newDryRunResult.cancelDryRun();
      },
    });

    // ステータス変更時に、今実行したクエリが最新のクエリなのかどうか調べて、
    // そうであるならばクエリ実行時刻を更新する
    const onStatusChange = status => {
      if (dryRunResultInExecution.current === newDryRunResult) {
        setDryRunState({ 
          dryRunUpdatedAt: newDryRunResult.getUpdatedAt(), 
          dryRunStatus: status
        });
      }
    };

    newDryRunResult
      .toPromise(onStatusChange)
      // 成功ケース
      .then(dryRunResult => {
        // 今見てるクエリの結果が実行中のものであるならば
        if (dryRunResultInExecution.current === newDryRunResult) {
          // クエリの実行結果は現在のクエリと同じものであるならば
          // ???
          // TODO: add dry_run_result field
          if (dryRunResult && dryRunResult.dry_run_result.query === query.query) {
            query.latest_query_data_id = dryRunResult.getId();
            // TODO: add dryRunResult field to query object
            query.dryRunResult = dryRunResult;
          }

          // ???けどブラウザ通知させる
          if (dryRunState.loadedInitialResults) {
            notifications.showNotification("Redash", `${query.name} updated.`);
          }

          // 実行完了のステータスとして更新
          setDryRunState({
            dryRunResult: dryRunResult,
            loadedInitialDryRunResults: true,
            dryRunError: null,
            isDryRunning: false,
            isDryRunCancelling: false,
            dryRunStatus: null,
          });
        }
      })
      .catch(dryRunResult => {
        // エラーになったとき
        if (dryRunResultInExecution.current === newDryRunResult) {
          if (dryRunState.loadedInitialResults) {
            notifications.showNotification("Redash", `${query.name} failed to run: ${dryRunResult.getDryRunError()}`);
          }

          // 失敗したステータスで状態を更新する
          setDryRunState({
            dryRunResult: dryRunResult,
            loadedInitialDryRunResults: true,
            dryRunError: dryRunResult.getError(),
            isDryRunning: false,
            isDryRunCancelling: false,
            // TODO: impl DryRunResults in dry-run-results.js
            dryRunStatus: DryRunResults.FAILED,
          });
        }
      });
  });

  // TODO: ドライラン向けに何らか修正が必要かもしれない
  const queryRef = useRef(query);
  queryRef.current = query;

  // 上から順に実行というわけではない
  useEffect(() => {
    // TODO: this belongs on the query page?
    // loadedInitialResults can be removed if so
    if (queryRef.current.hasResult() || queryRef.current.paramsRequired()) {
      dryRunQuery();
    } else {
      // 実行されたとき
      setDryRunState({ loadedInitialDryRunResults: true });
    }
  }, [dryRunQuery]);

  return { ...dryRunState, ...{ dryRunQuery: dryRunQuery } };
}
