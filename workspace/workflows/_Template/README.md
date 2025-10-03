# Workflow Template

This is the master template directory that gets copied when creating new cycles.

## Directory Structure

```
_Template/
├── notebooks/              # Step notebooks organized by stage
│   ├── Stage_01_Setup/    # Initial setup and validation
│   ├── Stage_02_Extract/  # Data extraction
│   ├── Stage_03_Process/  # Data processing
│   ├── Stage_04_Submit/   # Job submission
│   └── Stage_05_Monitor/  # Job monitoring
├── files/                 # Input and output files
│   ├── excel_configuration/
│   └── extracted_configurations/
└── logs/                  # Execution logs
```

## Adding New Stages

1. Create a new directory: `notebooks/Stage_XX_<Name>/`
2. Add step notebooks: `Step_XX_<Name>.ipynb`
3. Include a README.md in the stage directory

## Adding New Steps

Each step notebook should include:

1. **Header cell** with title and description
2. **Step tracker** initialization
3. **Main logic** wrapped in try/catch
4. **Completion** marking (success/failure)
5. **Verification** section

## Step Notebook Template

```python
# Step tracking
%run /home/jovyan/work/system/helpers/03_StepTracker.ipynb

step = StepTracker(
    cycle_name="{{CYCLE_NAME}}",
    stage_num=X,
    step_num=Y,
    idempotent=False,
    auto_start=True
)

if step.can_execute:
    try:
        # Your logic here
        step.complete_run(output_data)
    except Exception as e:
        step.fail_run(str(e))
        raise
```

## Naming Conventions

- Stages: `Stage_XX_<DescriptiveName>`
- Steps: `Step_XX_<DescriptiveName>.ipynb`
- XX = two-digit number (01, 02, etc.)

## Notes

- This template is copied when creating new cycles
- The placeholder `{{CYCLE_NAME}}` is replaced with actual cycle name
- Modifications here affect all future cycles