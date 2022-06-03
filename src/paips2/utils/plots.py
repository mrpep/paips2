import plotly.graph_objects as go

def generate_tooltip(task_name, task):
    hover_data_i = '{}:\n'.format(task_name)
    for k,v in task.config.items():
        hover_data_i += '\n{}: {}'.format(k,v)
    return hover_data_i

def sankey_plot(tasks, output_path):
    source = []
    target = []
    value = []
    label = []
    task_name_to_number = {}
    hover_data = []
    i = 0
    for task_name, task in tasks.items():
        if task_name not in task_name_to_number:
            task_name_to_number[task_name] = i
            i += 1
            label.append(task_name)
            hover_data_i = generate_tooltip(task_name, task)
            hover_data.append(hover_data_i)
        deps = task.get_dependencies()
        for dep in deps:
            if dep != 'self':
                if dep not in task_name_to_number:
                    task_name_to_number[dep] = i
                    i += 1
                    label.append(dep)
                    try:
                        hover_data_i = generate_tooltip(dep, tasks[dep])
                    except:
                        from IPython import embed; embed()
                    hover_data.append(hover_data_i)
                target.append(task_name_to_number[task_name])
                source.append(task_name_to_number[dep])
                value.append(1.0)

    node = dict(
            pad = 15,
            thickness = 15,
            line = dict(color = "black", width = 0.5),
            label =  label,
            customdata = hover_data,
            hovertemplate='%{customdata}<extra></extra>'
    )
    
    link = dict(source = source, target = target, value = value)
    data = go.Sankey(node=node, link = link)
    fig = go.Figure(data)
    #fig.show()

    if not output_path.parent.exists():
        output_path.parent.mkdir(parents=True)
    fig.write_html(str(output_path.absolute()))