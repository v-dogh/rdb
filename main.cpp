#include <rdb_mount.hpp>
#include <rdb_types.hpp>

using server = rdb::Schema<"Server",
	rdb::Topology<
		rdb::Field<"ServerID", rdbt::TimeUUID>
	>,
	rdb::Topology<
		rdb::Field<"Name", rdbt::String>,
		rdb::Field<"Key", rdbt::BinaryArray<32>>,
		rdb::Field<"Roles", rdbt::Uint64>,
		rdb::Field<"Members", rdbt::Uint64>,
		rdb::Field<"Channels", rdbt::Uint64>,
		rdb::Field<"Icon", rdbt::Binary>
	>
>;
using server_by_name = rdb::Schema<"ServerByName",
	rdb::Topology<rdb::Field<"Name", rdbt::String>>,
	rdb::Topology<rdb::Field<"ServerID", rdbt::TimeUUID>>
>;
using server_user_index = rdb::Schema<"ServerUserIndex",
	rdb::Topology<
		rdb::Field<"ServerID", rdbt::TimeUUID>
	>,
	rdb::Topology<
		rdb::Field<"Hierarchy", rdbt::Uint64, rdb::FieldType::Sort>,
		rdb::Field<"Role", rdbt::TimeUUID, rdb::FieldType::Sort>,
		rdb::Field<"UserID", rdbt::TimeUUID, rdb::FieldType::Sort>,
		rdb::Field<"Name", rdbt::String>
	>
>;

using channel = rdb::Schema<"Channel",
	rdb::Topology<
		rdb::Field<"ServerID", rdbt::TimeUUID>
	>,
	rdb::Topology<
		rdb::Field<"ID", rdbt::TimeUUID, rdb::FieldType::Sort>,
		rdb::Field<"Name", rdbt::String>,
		rdb::Field<"Type", rdbt::String>,
		rdb::Field<"ContentCounter", rdbt::Uint64>
	>
>;
using channel_by_name = rdb::Schema<"ChannelByName",
	rdb::Topology<
		rdb::Field<"ServerID", rdbt::TimeUUID>,
		rdb::Field<"Name", rdbt::String>
	>,
	rdb::Topology<rdb::Field<"ChannelID", rdbt::TimeUUID>>
>;

using role = rdb::Schema<"Role",
	rdb::Topology<
		rdb::Field<"ServerID", rdbt::RandUUID>
	>,
	rdb::Topology<
		rdb::Field<"ID", rdbt::TimeUUID, rdb::FieldType::Sort>,
		rdb::Field<"Name", rdbt::String>,
		rdb::Field<"Permissions", rdbt::Bitset<64>>,
		rdb::Field<"ChannelPermsAllow", rdbt::Bitset<64>>,
		rdb::Field<"ChannelPermsDeny", rdbt::Bitset<64>>,
		rdb::Field<"Hierarchy", rdbt::Uint64>,
		rdb::Field<"Color", rdbt::Uint32>
	>
>;
using channel_role = rdb::Schema<"ChannelRole",
	rdb::Topology<
		rdb::Field<"ServerID", rdbt::RandUUID>,
		rdb::Field<"ChannelID", rdbt::RandUUID>
	>,
	rdb::Topology<
		rdb::Field<"ID", rdbt::TimeUUID, rdb::FieldType::Sort>,
		rdb::Field<"ChannelPermsAllow", rdbt::Bitset<64>>,
		rdb::Field<"ChannelPermsDeny", rdbt::Bitset<64>>
	>
>;

using global_entity = rdb::Schema<"GlobalEntity",
	rdb::Topology<
		rdb::Field<"UserID", rdbt::TimeUUID>
	>,
	rdb::Topology<
		rdb::Field<"Name", rdbt::String>,
		rdb::Field<"Friends", rdbt::Buffer<rdbt::TimeUUID>>,
		rdb::Field<"Servers", rdbt::Buffer<rdbt::TimeUUID>>,
		rdb::Field<"OwnedServers", rdbt::Uint64>,
		rdb::Field<"Flags", rdbt::Bitset<32>>
	>
>;
using entity = rdb::Schema<"Entity",
	rdb::Topology<
		rdb::Field<"ServerID", rdbt::TimeUUID>
	>,
	rdb::Topology<
		rdb::Field<"UserID", rdbt::TimeUUID, rdb::FieldType::Sort>,
		rdb::Field<"Roles", rdbt::Buffer<rdbt::TimeUUID>>,
		rdb::Field<"Joined", rdbt::Timestamp>
	>
>;

using message = rdb::Schema<"Message",
	rdb::Topology<
		rdb::Field<"ServerID", rdbt::TimeUUID>,
		rdb::Field<"ChannelID", rdbt::TimeUUID>,
		rdb::Field<"DayBucket", rdbt::Timestamp>
	>,
	rdb::Topology<
		rdb::Field<"ID", rdbt::TimeUUID, rdb::FieldType::Sort, rdb::Order::Descending>,
		rdb::Field<"SenderID", rdbt::TimeUUID>,
		rdb::Field<"Flags", rdbt::Bitset<32>>,
		rdb::Field<"Message", rdbt::Binary>,
		rdb::Field<"Resources", rdbt::Nullable<rdbt::Buffer<rdbt::TimeUUID>>>
	>
>;
using attachment = rdb::Schema<"Attachment",
	rdb::Topology<
		rdb::Field<"ServerID", rdbt::TimeUUID>,
		rdb::Field<"ChannelID", rdbt::TimeUUID>
	>,
	rdb::Topology<
		rdb::Field<"ID", rdbt::TimeUUID, rdb::FieldType::Sort>,
		rdb::Field<"SenderID", rdbt::TimeUUID>,
		rdb::Field<"Data", rdbt::Binary>
	>
>;

int main()
{
	rdb::require<
		server,
		server_by_name,
		server_user_index,
		channel,
		channel_by_name,
		role,
		channel_role,
		global_entity,
		entity,
		message,
		attachment
	>();

	std::filesystem::remove_all("/tmp/RDB");

	rdb::Mount::ptr mnt = rdb::Mount::make(
		rdb::Config{
			.root = "/tmp/RDB"
		}
	);
	mnt->logs()->sink<rdb::rs::ConsoleSink>();
	mnt->start();

	const auto uid = rdb::uuid::uint128_t();
	const auto username = std::string_view("Test");

	mnt->query
		<< rdb::compose(
			rdb::fetch<global_entity>(uid)
			| rdb::reset
			| rdb::write<"Name">(username)
		)
		<< rdb::execute<>;

	const auto name = std::string_view("Test");
	const auto id = rdbt::TimeUUID::id();
	const auto created = rdbt::Timestamp::now();
	const auto key = std::span<const unsigned char>();
	const auto icon = std::span<const unsigned char>();

	bool result = false;
	mnt->query
		<< rdb::lock<server>(&result, id,
			rdb::pred(
				rdb::check<server_by_name>(&result, name)
				   < rdb::invert(rdb::exists),
				rdb::atomic(
					// Create server
					rdb::create<server>(
						id,
						name,
						key,
						1, 1, 0,
						icon
					),
					// Index by name
					rdb::create<server_by_name>(name, id),
					// // Increment creator server count and push server to list
					rdb::fetch<global_entity>(uid)
					| rdb::wproc<"OwnedServers", rdbt::Uint64::Op::Add>(1)
					| rdb::wproc<"Servers", rdbt::Buffer<rdbt::TimeUUID>::Op::Push>(id),
					// Create owner role
					rdb::create<role>(
						id, id,
						std::string_view("Owner"),
						rdbt::Bitset<64>::list{ true },
						rdbt::Bitset<64>::list{},
						rdbt::Bitset<64>::list{},
						0, ~std::uint32_t(0)
					),
					// Index user
					rdb::create<server_user_index>(
						id, 0, id, uid,
						username
					),
					// Give user the owner role
					rdb::create<entity>(id, uid, id, created)
				)
			)
		)
		<< rdb::execute<>;

	global_entity::interface_view<"Servers"> svs;
	mnt->query
		<< rdb::compose(
			rdb::fetch<global_entity>(uid)
			| rdb::read<"Servers">(&svs)
		)
		<< rdb::execute<>;
	std::cout << svs->print() << std::endl;

	mnt->wait();
}
