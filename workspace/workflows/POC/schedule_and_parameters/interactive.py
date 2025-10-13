def yes_no(prompt="Continue execution?"):
    """
    Simple yes/no prompt that blocks execution until user responds.
    Returns True for yes, False for no.
    """
    while True:
        response = input(f"{prompt} (y/n): ").strip().lower()
        if response in ['y', 'yes']:
            return True
        elif response in ['n', 'no']:
            return False
        print("Please enter 'y' or 'n'")

def _default_validation(text):
    """Default validation function that checks if input is not blank"""
    return bool(text.strip())

def text_input(prompt="Enter value:", default="", placeholder="", validation=None):
    """
    Simple text input that blocks execution until user submits valid input.
    
    Args:
        prompt: Text prompt to show to the user
        default: Default value to suggest
        placeholder: Example text to show in the prompt as a hint
        validation: Optional validation function that returns True/False
                   Defaults to non-blank validation if None
    
    Returns:
        str: The validated input text, or None if cancelled
    """
    # Use default validation if none provided
    if validation is None:
        validation = _default_validation
    
    # Build complete display prompt
    display_prompt = prompt
    
    # Add placeholder if provided
    if placeholder:
        display_prompt = f"{display_prompt} (e.g., {placeholder})"
    
    # Add default if provided
    if default:
        display_prompt = f"{display_prompt} [default: {default}]"
    
    # Print the complete prompt
    print(f"{display_prompt} (enter 'cancel' to stop)")
    
    while True:
        response = input("> ").strip()
        
        # Check for cancel
        if response.lower() == 'cancel':
            return None
        
        # Use default if empty response and default exists
        if not response and default:
            response = default
        
        # Validate the input
        if validation(response):
            return response
        else:
            print("Invalid input. Please try again or enter 'cancel' to stop.")

def dropdown(options, prompt="Select an option:", default=None):
    """
    Simple menu selection that blocks execution until user chooses.
    Returns the selected option, or None if cancelled.
    """
    if not options:
        return None
    
    default_option = options[default] if default and 0 <= default < len(options) else None
    display_prompt = prompt
    if default_option:
        display_prompt = f"{prompt} [default: {default_option}]"
    
    print(f"{display_prompt}")
    print("Options:")
    for i, option in enumerate(options):
        print(f"{i+1}. {option}")
    print(f"Enter 1-{len(options)} or 'cancel' to stop")
    
    while True:
        response = input("> ").strip()
        
        # Check for cancel
        if response.lower() == 'cancel':
            return None
            
        # Use default if empty and default exists
        if not response and default_option:
            return default_option
        
        try:
            idx = int(response) - 1
            if 0 <= idx < len(options):
                return options[idx]
        except ValueError:
            pass
        print(f"Please enter a number between 1 and {len(options)}")