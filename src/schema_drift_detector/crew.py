from crewai import Agent, Crew, Process, Task, LLM
from crewai.project import CrewBase, agent, crew, task
from typing import List
import os

# Import the tools we created
from schema_drift_detector.tools.agent_tools import (
    SourceSchemaIdentifierTool,
    CSVCrawlerTool,
    DatabaseCrawlerTool,
    APICrawlerTool,
    MetadataPersistenceTool,
    DriftDetectionTool,
    HealingTool,
    NotificationTool
)

@CrewBase
class SchemaDriftDetector():
    """SchemaDriftDetector crew"""

    agents_config = 'config/agents.yaml'
    tasks_config = 'config/tasks.yaml'

    def __init__(self):
        # Initialize Gemini LLM
        # Ensure GEMINI_API_KEY and GEMINI_MODEL are set in environment
            self.llm = LLM(
                model=f"gemini/{os.getenv('GEMINI_MODEL', 'gemini-1.5-flash')}",
                api_key=os.getenv("GEMINI_API_KEY")
            )

    @agent
    def orchestrator_agent(self) -> Agent:
        return Agent(
            config=self.agents_config['orchestrator_agent'],
            verbose=True,
            allow_delegation=True,
            llm=self.llm
        )

    @agent
    def source_schema_identifier(self) -> Agent:
        return Agent(
            config=self.agents_config['source_schema_identifier'],
            tools=[SourceSchemaIdentifierTool()],
            verbose=True,
            llm=self.llm
        )

    @agent
    def database_crawler_agent(self) -> Agent:
        return Agent(
            config=self.agents_config['database_crawler_agent'],
            tools=[DatabaseCrawlerTool()],
            verbose=True,
            llm=self.llm
        )

    @agent
    def api_crawler_agent(self) -> Agent:
        return Agent(
            config=self.agents_config['api_crawler_agent'],
            tools=[APICrawlerTool()],
            verbose=True,
            llm=self.llm
        )

    @agent
    def csv_crawler_agent(self) -> Agent:
        return Agent(
            config=self.agents_config['csv_crawler_agent'],
            tools=[CSVCrawlerTool()],
            verbose=True,
            llm=self.llm
        )

    @agent
    def snapshot_persistence_agent(self) -> Agent:
        return Agent(
            config=self.agents_config['snapshot_persistence_agent'],
            tools=[MetadataPersistenceTool()],
            verbose=True,
            llm=self.llm
        )

    @agent
    def detector_agent(self) -> Agent:
        return Agent(
            config=self.agents_config['detector_agent'],
            tools=[DriftDetectionTool()],
            verbose=True,
            llm=self.llm
        )

    @agent
    def healer_agent(self) -> Agent:
        return Agent(
            config=self.agents_config['healer_agent'],
            tools=[HealingTool()],
            verbose=True,
            llm=self.llm
        )

    @agent
    def notification_agent(self) -> Agent:
        return Agent(
            config=self.agents_config['notification_agent'],
            tools=[NotificationTool()],
            verbose=True,
            llm=self.llm
        )

    @task
    def identify_sources(self) -> Task:
        return Task(
            config=self.tasks_config['identify_sources'],
        )

    @task
    def crawl_database(self) -> Task:
        return Task(
            config=self.tasks_config['crawl_database'],
        )

    @task
    def crawl_api(self) -> Task:
        return Task(
            config=self.tasks_config['crawl_api'],
        )

    @task
    def crawl_csv(self) -> Task:
        return Task(
            config=self.tasks_config['crawl_csv'],
        )

    @task
    def persist_snapshots(self) -> Task:
        return Task(
            config=self.tasks_config['persist_snapshots'],
        )

    @task
    def detect_drift(self) -> Task:
        return Task(
            config=self.tasks_config['detect_drift'],
        )

    @task
    def generate_healing(self) -> Task:
        return Task(
            config=self.tasks_config['generate_healing'],
        )

    @task
    def notify_operator(self) -> Task:
        return Task(
            config=self.tasks_config['notify_operator'],
        )

    @task
    def finalize_decision(self) -> Task:
        return Task(
            config=self.tasks_config['finalize_decision'],
        )

    @crew
    def crew(self) -> Crew:
        """Creates the SchemaDriftDetector crew"""
        return Crew(
            agents=self.agents, # Automatically created by the @agent decorator
            tasks=self.tasks, # Automatically created by the @task decorator
            process=Process.sequential,
            verbose=True,
        )
