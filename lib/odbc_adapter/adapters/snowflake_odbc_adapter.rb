require 'odbc_adapter/adapters/postgresql_odbc_adapter'

module ODBCAdapter
  module Adapters
    # Overrides specific to Snowflake. Mostly taken from
    # https://eng.localytics.com/connecting-to-snowflake-with-ruby-on-rails/
    class SnowflakeODBCAdapter < PostgreSQLODBCAdapter
      # Explicitly turning off prepared statements as they are not yet working with
      # snowflake + the ODBC ActiveRecord adapter
      def prepared_statements
        false
      end

      # Quoting needs to be changed for snowflake
      def quote_column_name(name)
        name.to_s
      end

      # Override all the schema fetching methods from
      # `ODBCAdapter::SchemaStatements` to return empty arrays. These are
      # currently making expensive calls to snowflake and returning  empty
      # arrays anyway. Until we figure out how to make them work correctly, at
      # least make them fast.
      def tables(*args)
        []
      end

      def views(*args)
        []
      end

      def indexes(*args)
        []
      end

      def columns(*args)
        []
      end

      def primary_key(*args)
        []
      end

      def foreign_keys(*args)
        []
      end

      private

      # Override dbms_type_cast to get the values encoded in UTF-8
      def dbms_type_cast(columns, values)
        int_column = {}
        columns.each_with_index do |c, i|
          int_column[i] = c.type == 3 && c.scale.zero?
        end

        float_column = {}
        columns.each_with_index do |c, i|
          float_column[i] = c.type == 3 && !c.scale.zero?
        end

        values.each do |row|
          row.each_index do |idx|
            val = row[idx]
            if val
              if int_column[idx]
                row[idx] = val.to_i
              elsif float_column[idx]
                row[idx] = val.to_f
              elsif val.is_a?(String)
                row[idx] = val.force_encoding('UTF-8')
              end
            end
          end
        end
      end
    end
  end
end
