from paips2.core import Task

class LeaveOneOut(Task):
    def get_valid_parameters(self):
        return ['in', 'column'], []

    def get_output_names(self):
        return ['train','validation']

    def process(self):
        folds = list(sorted(self.config['in'][self.config['column']].unique()))
        training_folds = []
        validation_folds = []
        for fold in folds:
            validation_folds.append([fold])
            training_folds.append([x for x in folds if x != fold])
        return training_folds, validation_folds


