{% macro log_dq_results(results) %}
  {#- Runs after every dbt command (on-run-end). Appends one row per test to dq_test_history. -#}
  {% if execute %}
    {% set tests = [] %}
    {% for r in results %}
      {% if r.node.resource_type == 'test' %}
        {% do tests.append(r) %}
      {% endif %}
    {% endfor %}

    {% if tests | length > 0 %}
      {% set history = target.database ~ "." ~ target.schema ~ ".dq_test_history" %}

      {% do run_query(
        "create table if not exists " ~ history ~ " (
           invocation_id     string,
           run_started_at    timestamp,
           test_name         string,
           model_name        string,
           severity          string,
           status            string,
           failures          bigint,
           execution_seconds double)"
      ) %}

      {% set rows = [] %}
      {% for r in tests %}
        {#- Find the model this test checks (first 'model.' dependency) -#}
        {% set ns = namespace(model='') %}
        {% for n in r.node.depends_on.nodes %}
          {% if n.startswith('model.') and ns.model == '' %}
            {% set ns.model = n.split('.')[-1] %}
          {% endif %}
        {% endfor %}
        {% set failures = r.failures if r.failures is not none else 'null' %}
        {% do rows.append(
          "('" ~ invocation_id ~ "', "
          ~ "timestamp'" ~ run_started_at.strftime('%Y-%m-%d %H:%M:%S') ~ "', "
          ~ "'" ~ r.node.name ~ "', "
          ~ "'" ~ ns.model ~ "', "
          ~ "'" ~ r.node.config.severity | lower ~ "', "
          ~ "'" ~ r.status ~ "', "
          ~ failures ~ ", "
          ~ r.execution_time ~ ")"
        ) %}
      {% endfor %}

      {% do run_query("insert into " ~ history ~ " values " ~ rows | join(", ")) %}
      {{ log("Logged " ~ tests | length ~ " test results to " ~ history, info=True) }}
    {% endif %}
  {% endif %}
{% endmacro %}
