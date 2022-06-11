CREATE TABLE product_list (
	id text primary key,
	domain text not null,
	name text not null,
	url text not null,
	created_date date not null
)
CREATE TABLE product (
	id text primary key,
	raw_id text REFERENCES product_list(id),
	domain text not null,
	name text not null,
	trademark text,
	OS text,
	CPU text,
	RAM int,
	ROM int,
	screen_type  text,
	screen_size int,
	screen_resolution_x int,
	screen_resolution_y int,
	num_of_camera int,
	battery_capacity int,
	battery_type text,
	support_sdcard boolean,
	support_5g boolean,
	num_of_sim int,
	charging_port text,
	flash boolean
)