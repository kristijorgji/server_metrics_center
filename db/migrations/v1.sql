create schema server_metrics collate utf8_general_ci;
use server_metrics;
create table metrics
(
	date datetime not null,
	used_memory float not null,	
	free_memory float not null,	
	available_memory float not null,	
	primary key (date)
);