import type { WorkflowStep, StepStatus } from "../types/workflow"

export function getReadySteps(
  steps: WorkflowStep[],
  stepStatus: Record<string, StepStatus>
): WorkflowStep[] {

  return steps.filter((x)=>{
    const currentstatus = stepStatus[x.id];
    if(currentstatus === 'QUEUED'){
      return false
    }
    if(currentstatus === 'COMPLETED'){
      return false
    }
    if(currentstatus === 'RUNNING'){
      return false
    }
    if(currentstatus === "FAILED"){
      return false
    }
    if(currentstatus === 'SKIPPED'){
      return false
    }
    if(!x.dependsOn || x.dependsOn.length == 0){
      return true
    }
    for(const depndency of x.dependsOn){
      if(stepStatus[depndency] !== 'COMPLETED'){
        return false;
      }
    }
    return true
  })
}
