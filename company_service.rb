class V0::CompanyService
    include Elasticsearch::Model
  
    SELECT_FIELDS = {
      include: [
        :id, :name, :homepage_url, :linkedin_url, :industry, :revenue, :logo_url, :location_area, :employee_range,
        :watchlisted_users, :contacts_count_, :contacts_count_true, :contacts_count_false, :linkedin_name,
        :city_name, :state_name, :country_name, :employees_linkedin, :employee_low, :li_updated_at, :date_founded,
        :ticker, :revenue_high, :revenue_low, :annual_cost_of_revenue, :annual_net_income, :annual_operating_income,
        :annual_gross_profit, :fiscal_year, :phone, :street_address, :sic_codes, :sic_explanations, :company_location,
        :description, :es_web_traffic_data, :es_seo_keywords, :es_web_technographics, :zip_code
      ]
    }
  
    CRITERIA = {
      technologies: "technologies.id",
      categories: "categories.id",
      rankings: "rankings.id",
      company_types: "company_type_normalized",
      status: "status",
      industries: "industry",
      zip_code: 'zip_code'
    }
  
    class << self
  
      def company_id_from_company_name(company_name)
        query = {
          _source: { include: [ :id ] },
          size: 1,
          sort: [ { id: { order: :asc } } ],
          query: {
            bool: { must: [
              { bool: { should: [ { term: { subsidiary: false } }, { bool: { must_not: { exists: { field: "subsidiary" } } } } ] } },
              { query_string: { default_field: "linkedin_name", query: company_name, split_on_whitespace: false, default_operator: "and" } }
            ] }
          }
        }
        total = Company.search(query).results.total
        return Company.search(query).results.first[:_source][:id] if total > 0
        nil
      end
  
      def default_list(size, employee_low, user_id, is_current)
        query = {
          _source: SELECT_FIELDS,
          size: size,
          sort: [ { popularity: { order: :asc, unmapped_type: "long" } } ],
          query: { bool: {
            must: [ { range: { employee_low: { gte: employee_low } } } ],
            must_not: [ { match: { subsidiary: true } } ]
          }
          }
        }
        generate_companies_data(Company.search(query), user_id, is_current, true)[:companies]
      end
  
      def industry_autocomplete(match_string, autocomplete_limit)
        query = {
          size: autocomplete_limit,
          query: {
            match_phrase_prefix: { industry: match_string },
          },
          aggs: {
            distinct_industries: {
              terms: {
                field: 'industry.keyword',
                size: 10000
              },
              aggs: {
                top_industry_hits: {
                  top_hits: {
                    size: 1,
                    _source: { includes: [:industry] },
                  }
                }
              }
            }
          }
        }
        Company.search(query).aggregations.distinct_industries.buckets.map{|b| b[:key]}.sort
      end
  
      def default_industries
        query =  {:bool=>{:must=>[{:exists=>{:field=>"linkedin_url"}}, {:exists=>{:field=>"industry"}}]}}
        aggs = {:distinct_industries=>{:terms=>{:field=>"industry.keyword", :size=>10000}}}
        results = Company.search(aggs: aggs, query: query).aggregations.distinct_industries.buckets
        industries = results.as_json
        industries = industries.select{|a| a["key"] if(a["doc_count"] > 100 && !a["key"].blank?)}
        industries.map{|industry| industry["key"]}.sort
      end
  
      def generate_industry_autocomplete_data(search_result)
        industry_data = search_result.collect do |company|
          company[:_source][:industry]
        end
        industry_data
      end
  
      def company_autocomplete(params, size, offset, user_id, is_current)
        match_string = params[:q]
        query = {
          _source: SELECT_FIELDS,
          from: offset, size: size,
          sort: [ { 'name.keyword' => { order: :asc } } ],
          query: {
            bool: {
              must: [ { match_phrase_prefix: { name: match_string } } ],
              must_not: [ { match: { subsidiary: true } } ],
              filter: { bool: { must: get_range_query_autocomplete(params) } }
            }
          }
        }
        generate_companies_data(Company.search(query), user_id, is_current, false)
      end
  
      def generate_companies_data(search_results, user_id, is_current, include_total_results, scroll: false)
        companies_data = {}
        if scroll.eql? true
          companies_data[:scroll_id] = search_results['_scroll_id']
          companies_data[:total_results] = search_results["hits"]["total"]
          results =  search_results["hits"]["hits"].map{|a| a["_source"].with_indifferent_access}
        else
          companies_data = { total_results: search_results.results.total }
          results = search_results.results.collect(&:_source)
        end
        companies = results.collect do |source|
          source[:is_watchlisted] = (source[:watchlisted_users].include? user_id) ? true : false
          source.delete("watchlisted_users")
          source[:is_fresh] = source['li_updated_at'].present? ? (Time.now - DateTime.parse(source['li_updated_at'])) < Company::DATA_REFRESH_AFTER : false
          source
        end
        companies = companies.collect do |company_data|
          filter_contacts_count(company_data, is_current)
        end
        companies_data[:companies] = companies
        return companies_data if include_total_results
        companies
      end
  
      def get_range_query_autocomplete(params)
        range_query = [ { range: { popularity: {gt: 0} } } ]
        range_query << { range: { employee_low: { gte: params[:employee_low] } } } if params[:employee_low].present?
        range_query
      end
  
      def company_advance_elasticsearch(criteria, start_index, size, user, is_current, scroll: false, scroll_id: nil, select_fields: nil)
        if select_fields.present?
          select_fields = {include: select_fields + [:watchlisted_users, :contacts_count_, :contacts_count_true, :contacts_count_false]}
        else
          select_fields = SELECT_FIELDS
        end
        return generate_companies_data(Elasticsearch::Model::client.scroll(scroll_id: scroll_id, scroll: "2m"), user.id, is_current, is_current, scroll: true) if scroll_id.present?
        terms_query_result = get_terms_query(criteria)
        terms_query = terms_query_result[:terms_q]
        category_should_query = terms_query_result[:category_q]
        query = {
          _source: select_fields,
          sort: [ { popularity: { order: :asc, unmapped_type: "long" }, id: { order: :asc } } ],
          query: {
            bool: {
              must: terms_query,
              filter: {
                bool: {
                  must: get_filter_query_advance(criteria, category_should_query: category_should_query),
                  must_not: get_must_not_query(criteria)
                }
              }
            }
          }
        }
        query[:query][:bool].merge!({should: add_location_query(criteria), minimum_should_match: 1}) if criteria["locations"].present?
        query[:query][:bool][:must].append({"ids":{"values":criteria["ids"]}}) if criteria["ids"].present?
        
        if scroll.blank?
          query.merge!(from: start_index, size: size)
          return generate_companies_data(Company.search(query), user.id, is_current, true)
        end
  
        if scroll_id.blank?
          return generate_companies_data(Elasticsearch::Model.client.search(index: "companies_#{Rails.env}",
                                                                            scroll: '2m',
                                                                            size: size,
                                                                            body: query), user.id, is_current, true, scroll: true)
  
        end
      end
  
      def company_matches_with_criteria?(company, criteria)
        terms_query_result = get_terms_query(criteria)
        terms_query = terms_query_result[:terms_q]
        category_should_query = terms_query_result[:category_q]
        filter_query = get_filter_query_advance(criteria, category_should_query: category_should_query)
        filter_query << { term: { id: company.id } }
        query = {
          query: {
            bool: {
              must: terms_query,
              filter: {
                bool: {
                  must: filter_query,
                  must_not: get_must_not_query(criteria)
                }
              }
            }
          }
        }
        query[:query][:bool].merge!({should: add_location_query(criteria), minimum_should_match: 1}) if criteria["locations"].present?
        return true if Company.search(query).results.total > 0
        false
      end
  
      def get_must_not_query(criteria)
        criteria = criteria.with_indifferent_access
        must_not = []
        if criteria.key?('industry_exclusions') && criteria['industry_exclusions'].present?
          must_not << { terms: { "industry.keyword" => criteria['industry_exclusions'] } }
        end
        if criteria.key?('category_exclusions') && criteria['category_exclusions'].present?
          must_not << { terms: { CRITERIA[:categories] => get_category_exclusion_ids(criteria) } }
        end
        must_not << { match: { subsidiary: true } } unless criteria.key?(:subsidiary)
        return must_not
      end
  
      def get_terms_query(criteria)
        criteria = criteria.with_indifferent_access
        category_names = criteria['categories']
        cr = add_ids_in_criteria(criteria)
        cr = cr.with_indifferent_access
        terms_query = CRITERIA.collect do |key, value|
          next if key.to_s.eql?('categories')
          values = cr[key].to_a.collect {|val| do_downcase(val)}
          { terms: { value => values } } if cr.key?(key)
        end
        category_query = category_names.present? ? { bool: { should: category_should_query(cr['categories'], category_names), minimum_should_match: 1 } } : nil
        { terms_q: terms_query.compact, category_q: category_query }
      end
  
      def category_should_query(category_ids, category_names)
        query = [ { terms: { 'categories.id' => category_ids } } ]
        category_names.each do |category_name|
          query << { match: { name: { query: category_name, operator: "and" } } }
          query << { match: { description: { query: category_name, operator: "and" } } }
        end
        query
      end
  
      def add_location_query(criteria)
        query = []
        criteria["locations"].each do |location|
          l_query = []
          l_query << {term: { 'city_id' => location["city_id"]}} if location["city_id"].present?
          l_query << {term: { 'state_id' => location["state_id"]}} if location["state_id"].present?
          l_query << {term: { 'country_id' => location["country_id"]}} if location["country_id"].present?
          query << {bool: {must: l_query}}
        end
        query
      end
  
      def do_downcase(value)
        return value.downcase if value.respond_to?(:downcase)
        value
      end
  
      def get_category_exclusion_ids(cr)
        return [] if cr['category_exclusions'].blank?
        cr['categories_exclusions'] = get_ids_from_names('category_exclusions', Category, cr)
        cr['categories_exclusions']
      end
  
      def add_ids_in_criteria(cr)
        [{categories: Category}, {technologies: Technology}, {rankings: Ranking}].each do |hash|
          key = hash.keys.first
          value = hash.values.first
          cr[key] = get_ids_from_names(key, value, cr) if cr[key].present?
        end
        cr
      end
  
      def get_ids_from_names(category, klass, criteria)
        return category_query(criteria[category]) if klass == Category
        values = criteria[category].collect { |val| val.downcase }
        query = {
          _source: { include: [:id] },
          query: { terms: { name: values } }
        }
        klass.search(query).results.collect{ |result| result[:_source][:id] }
      end
  
      def category_query(categories)
        query = {
          _source: { include: [:id]},
          query: { terms: { 'name.keyword' => categories.map(&:downcase) } }
        }
        Category.search(query).results.collect{ |result| result[:_source][:id] }
      end
  
      def get_filter_query_advance(criteria, category_should_query: nil)
        range_query = []
  
        if criteria.key?(:last_updated)
          range_query << { range: { updated_at: { gte: criteria[:last_updated] } } }
        end
        if criteria.key?(:custom_employee_range)
          range_query << { range: { employees_linkedin: { gte: criteria[:custom_employee_range][:low] } } } if criteria[:custom_employee_range].key?(:low)
          range_query << { range: { employees_linkedin: { lte: criteria[:custom_employee_range][:high] } } } if criteria[:custom_employee_range].key?(:high)
        end
        if criteria.key?(:employee)
          range_query << { range: { employee_low: { gte: criteria[:employee][:low] } } } if criteria[:employee].key?(:low)
          range_query << { range: { employee_high: { lte: criteria[:employee][:high] } } } if criteria[:employee].key?(:high)
        end
        if criteria.key?(:revenue)
          range_query << { range: { revenue_low: { gte: criteria[:revenue][:low] } } } if criteria[:revenue].key?(:low)
          range_query << { range: { revenue_high: { lte: criteria[:revenue][:high] } } } if criteria[:revenue].key?(:high)
        end
  
        if criteria.key?(:verified_at)
          date = criteria[:verified_at].is_a?(Integer) ?
            criteria[:verified_at].days.ago : criteria[:verified_at]
  
          range_query << { range: { li_updated_at: { gte: date } } }
        end
  
        if category_should_query.present?
          range_query << { bool: { filter: category_should_query } }
        end
  
        range_query << {term: { "subsidiary" => criteria[:subsidiary] } } if criteria.key?(:subsidiary)
  
        range_query
      end
  
      def filter_contacts_count(company_data, is_current)
        company_data[:contacts_count] = company_data["contacts_count_#{is_current}".to_sym]
        [nil, true, false].each do |is_current_value|
          company_data.delete("contacts_count_#{is_current_value}".to_sym)
        end
        company_data
      end
  
      def companies_by_company_ids(company_ids, user_id, is_current)
        company_data = company_ids.each_slice(ELASTICSEARCH_MAX_RESTULS).map do |ids|
          query = {
            _source: SELECT_FIELDS,
            size: ids.size,
            query:
            {
              constant_score:
              {
                filter:
                {
                  terms:
                  {
                    _id: ids
                  }
                }
              }
            }
          }
          generate_companies_data(Company.search(query), user_id, is_current, true)
        end
        final_res = {total_results: 0, companies: []}
        company_data.each do |data|
          final_res[:total_results] += data[:total_results]
          final_res[:companies] += data[:companies]
        end
        final_res
      end
  
      def get_data_for_watchlist(company_ids, size)
        select_fields = {
          include: [:id, :name, :homepage_url, :linkedin_url, :employee_low, :employees_linkedin, :industry, :date_founded,
                    :revenue, :li_updated_at, :linkedin_name, :logo_url, :location_area, :employee_range, :city_name, :state_name,
                    :country_name]
        }
        query = {
          _source: select_fields, size: size,
          query: { terms: { id: company_ids } }
        }
        Company.search(query).results.map{|company| company[:_source]}
      end
  
      def company_data_from_id(id, fields)
        return nil if id.blank?
        query = {
          _source: { include: fields },
          query: { term: { id: id } }
        }
        Company.search(query).results.first[:_source]
      end
    end
  end
  