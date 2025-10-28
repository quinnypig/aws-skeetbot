#!/usr/bin/env python3
"""
Monitor script for AWS Skeetbot
Checks key metrics and alerts on anomalies
"""

import boto3
import sys
from datetime import datetime, timedelta
from rich.console import Console
from rich.table import Table

console = Console()

def get_recent_metrics(namespace, metric_name, dimensions=None, period_minutes=60):
    """Get recent CloudWatch metrics"""
    cloudwatch = boto3.client('cloudwatch', region_name='us-west-2')
    
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(minutes=period_minutes)
    
    params = {
        'Namespace': namespace,
        'MetricName': metric_name,
        'StartTime': start_time,
        'EndTime': end_time,
        'Period': 300,  # 5-minute periods
        'Statistics': ['Sum', 'Average', 'Maximum']
    }
    
    if dimensions:
        params['Dimensions'] = dimensions
    
    response = cloudwatch.get_metric_statistics(**params)
    return response['Datapoints']

def check_lambda_health():
    """Check Lambda function health"""
    console.print("\n[bold cyan]Lambda Function Health Check[/bold cyan]")
    
    function_name = 'skeetbot-AwsWhatsNew-jmoc3Yk4R7VK'
    
    # Check various metrics
    metrics_to_check = [
        ('AWS/Lambda', 'Invocations', 'Total invocations'),
        ('AWS/Lambda', 'Errors', 'Errors'),
        ('AWS/Lambda', 'Duration', 'Duration (ms)'),
        ('AWS/Lambda', 'ConcurrentExecutions', 'Concurrent executions'),
    ]
    
    table = Table(title=f"Lambda Metrics (Last Hour)")
    table.add_column("Metric", style="cyan")
    table.add_column("Total", style="green")
    table.add_column("Average", style="yellow")
    table.add_column("Maximum", style="red")
    
    for namespace, metric, display_name in metrics_to_check:
        dimensions = [{'Name': 'FunctionName', 'Value': function_name}]
        datapoints = get_recent_metrics(namespace, metric, dimensions)
        
        if datapoints:
            total = sum(dp['Sum'] for dp in datapoints)
            avg = sum(dp['Average'] for dp in datapoints) / len(datapoints) if datapoints else 0
            maximum = max(dp['Maximum'] for dp in datapoints) if datapoints else 0
            
            table.add_row(
                display_name,
                f"{total:.0f}",
                f"{avg:.2f}",
                f"{maximum:.2f}"
            )
        else:
            table.add_row(display_name, "N/A", "N/A", "N/A")
    
    console.print(table)

def check_custom_metrics():
    """Check custom application metrics"""
    console.print("\n[bold cyan]Custom Application Metrics[/bold cyan]")
    
    metrics_to_check = [
        ('AnthropicRequests', 'Anthropic API calls'),
        ('ItemsProcessed', 'RSS items processed'),
        ('FailedPosts', 'Failed posts'),
        ('HighAPIUsage', 'High API usage alerts'),
        ('CircuitBreakerOpen', 'Circuit breaker activations'),
    ]
    
    table = Table(title="Application Metrics (Last Hour)")
    table.add_column("Metric", style="cyan")
    table.add_column("Total", style="green")
    table.add_column("Status", style="yellow")
    
    namespace = 'skeetbot'
    alerts = []
    
    for metric, display_name in metrics_to_check:
        datapoints = get_recent_metrics(namespace, metric)
        
        if datapoints:
            total = sum(dp['Sum'] for dp in datapoints)
            
            # Determine status
            status = "✅ OK"
            if metric == 'AnthropicRequests' and total > 50:
                status = "⚠️  High"
                alerts.append(f"High Anthropic API usage: {total} calls")
            elif metric == 'FailedPosts' and total > 0:
                status = "❌ Failed"
                alerts.append(f"Posts failed: {total}")
            elif metric == 'CircuitBreakerOpen' and total > 0:
                status = "🚨 Tripped"
                alerts.append(f"Circuit breaker activated {total} times")
            
            table.add_row(display_name, f"{total:.0f}", status)
        else:
            table.add_row(display_name, "0", "✅ OK")
    
    console.print(table)
    
    if alerts:
        console.print("\n[bold red]⚠️  Alerts:[/bold red]")
        for alert in alerts:
            console.print(f"  • {alert}")

def check_dynamodb_health():
    """Check DynamoDB table health"""
    console.print("\n[bold cyan]DynamoDB Health Check[/bold cyan]")
    
    dynamodb = boto3.client('dynamodb', region_name='us-west-2')
    
    table_name = 'skeetbot-AwsNewsRecentPostsTable-1CDB4R52SVMWP'
    
    try:
        # Get table description
        response = dynamodb.describe_table(TableName=table_name)
        table = response['Table']
        
        table_display = Table(title="DynamoDB Status")
        table_display.add_column("Property", style="cyan")
        table_display.add_column("Value", style="green")
        
        table_display.add_row("Table Status", table['TableStatus'])
        table_display.add_row("Item Count", str(table['ItemCount']))
        table_display.add_row("Size (bytes)", f"{table['TableSizeBytes']:,}")
        
        console.print(table_display)
        
    except Exception as e:
        console.print(f"[red]Error checking DynamoDB: {e}[/red]")

def main():
    """Main monitoring function"""
    console.print("[bold magenta]AWS Skeetbot Health Monitor[/bold magenta]")
    console.print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        check_lambda_health()
        check_custom_metrics()
        check_dynamodb_health()
        
        console.print("\n[bold green]✅ Monitoring complete[/bold green]")
    except Exception as e:
        console.print(f"\n[bold red]❌ Monitoring failed: {e}[/bold red]")
        sys.exit(1)

if __name__ == "__main__":
    main()