import type { QueuedStep } from "../types/workflow"
import { podPool } from "../k8s/pod-pool"
import { resultQueue } from "../queue/result-queue"

/**
 * TODO: Implement this class.
 *
 * The pod manager receives a step, runs it inside a leased pod,
 * and publishes status events to the result queue.
 *
 * dispatch(step) must:
 *   1. Call podPool.acquirePod() to lease a free pod
 *   2. Push { status: "RUNNING", podId, stepId, workflowId } to resultQueue
 *   3. Call podPool.execInPod(podId, step.command)
 *   4. Push { status: "COMPLETED", stdout, exitCode: 0 } to resultQueue
 *   5. Call podPool.releasePod(podId)
 *
 *   On any error:
 *   4. Push { status: "FAILED", error: err.message } to resultQueue
 *   5. Call podPool.releasePod(podId)  ← always release in finally block
 *
 * IMPORTANT: Pod manager never reads or writes workflow state.
 * It only pushes to the result queue.
 */
export class PodManager {
  async dispatch(step: QueuedStep): Promise<void> {
    const pod = await podPool.acquirePod()
    try{
      await resultQueue.push({
        stepId : step.stepId,
        workflowId: step.workflowId,
        status : "RUNNING",
        podId: pod.podId
      })
      const executedres = await podPool.execInPod(pod.podId , step.command)
      await resultQueue.push({
        stepId: step.stepId,
        workflowId: step.workflowId,
        status : "COMPLETED",
        podId : pod.podId,
        stdout: executedres,
        exitCode: 0
      })
    }catch(e){
      await resultQueue.push({
        stepId: step.stepId,
        workflowId: step.workflowId,
        status: "FAILED",
        podId: pod.podId,
        error: String(e)
      })
    }finally{
      podPool.releasePod(pod.podId)
    }
  }
}

export const podManager = new PodManager()
