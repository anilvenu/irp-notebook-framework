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
3. **Main logic**
4. **Completion** marking (success/failure)
5. **Verification** section

## Naming Conventions
- Cycle: `Active_<CycleName>`
- Stages: `Stage_XX_<DescriptiveName>`
- Steps: `Step_XX_<DescriptiveName>.ipynb`
- XX = two-digit number (01, 02, etc.)

## Notes

- This template is copied from _Template when creating new cycles
- Modifications in _Template affect all future cycles