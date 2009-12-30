

class LogReader():
    
    main_log_file = None
    main_log_path = None
    worker_logs = None
    
    def __init__(main_log_path):
        self.main_log_path = main_log_path
        self.worker_logs = []


    def close():
        """
        Closes any open file handles.
        """


    def __iter__():
        """
        Iterates lines in the aggregated file.
        """
        
        # lazy load the main file