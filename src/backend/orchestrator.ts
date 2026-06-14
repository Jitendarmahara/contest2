import type { StepStatus, Workflow, WorkflowState, WorkflowStep } from "../types/workflow";
import { getWorkflow, setWorkflow } from "./workflow-store";
import { getReadySteps } from "./dag";
import { stepQueue } from "../queue/step-queue";
import { resultQueue } from "../queue/result-queue";
import { podManager } from "../pod-manager/pod-manager";

export class Orchestrator {
  async submitWorkflow(workflow: Workflow): Promise<void> {
    // mark every step as pending to start
    const stepState: Record<string, any> = {};
    for (const step of workflow.steps) {
      stepState[step.id] = { stepId: step.id, status: "PENDING", podId: null };
    }
    const workflowState = {
      workflowId: workflow.workflowId,
      status: "running" as const,
      steps: workflow.steps,
      stepState,
    };
    setWorkflow(workflow.workflowId, workflowState);
    await this.removereadysteps(workflowState);
  }

  async start(): Promise<void> {
    const emptyloop = async () => {
      while (true) {
        const step = await stepQueue.dequeue();
        if (step) {
          podManager.dispatch(step)
        } else {
          await new Promise((r) => setTimeout(r, 100));
        }
      }
    };
    emptyloop().catch(console.error);
    await resultQueue.consume(async (result) => {
      const workflowState = getWorkflow(result.workflowId);
      if (!workflowState) return;

      const stepState = workflowState.stepState[result.stepId];
      if (!stepState) return;
      stepState.status = result.status;
      stepState.podId = result.podId;
      if (result.stdout !== undefined) stepState.stdout = result.stdout;
      if (result.exitCode !== undefined) stepState.exitCode = result.exitCode;
      if (result.error !== undefined) stepState.error = result.error;

      if (result.status === "COMPLETED") {
        await this.removereadysteps(workflowState);
      }
      if(result.status === "FAILED"){
        const dependes  = this.getDependency(result.stepId , workflowState.steps);
        for(const depid of dependes){
          workflowState.stepState[depid].status = 'SKIPPED'
        }
      }
      const allSteps = Object.values(workflowState.stepState);
      let allDone = true;

      for (const step of allSteps) {
        if (step.status !== "COMPLETED" &&step.status !== "FAILED" &&step.status !== "SKIPPED") {
          allDone = false;
          break;
        }
      }
      if (allDone) {
        let anyFailed = false;
        let faliedline = "" ;
        for (const step of allSteps) {
          if (step.status === "FAILED") {
            anyFailed = true;
            faliedline = step.stepId;
            break;
          }
        }
        if (anyFailed) {
          // traverse and find all the other dependes and marked them as skiped;
          workflowState.status = "failed";
        } else {
          workflowState.status = "completed";
        }
      }
      setWorkflow(result.workflowId, workflowState);
    });
  }
  private getDependency(failedid:string , steps : WorkflowStep[]){
    const result: string[] = [];
    for(const step of steps){
      if(step.dependsOn?.includes(failedid)){
        result.push(step.id)
        result.push(... this.getDependency(step.id , steps))
      }
    }
    return result;
  }

  private async removereadysteps(workflowState: WorkflowState): Promise<void> {
    const statusById: Record<string, StepStatus> = {};
    for (const [id, state] of Object.entries(workflowState.stepState)) {
      statusById[id] = state.status;
    }
    const readySteps = getReadySteps(workflowState.steps, statusById);
    for (const step of readySteps) {
      workflowState.stepState[step.id].status = "QUEUED";
      setWorkflow(workflowState.workflowId, workflowState);
      await stepQueue.enqueue({
        stepId: step.id,
        workflowId: workflowState.workflowId,
        command: step.command,
        enqueuedAt: Date.now(),
      });
    }
  }
}

export const orchestrator = new Orchestrator();
