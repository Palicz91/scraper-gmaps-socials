import os

def read_text_file_to_list(filename):
    """
    Read a text file and return its contents as a list of lines.
    Strips whitespace and filters out empty lines.
    """
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            lines = file.readlines()
            # Strip whitespace and filter out empty lines
            data = [line.strip() for line in lines if line.strip()]
            return data
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
        return []
    except Exception as e:
        print(f"Error reading file '{filename}': {e}")
        return []

def get_all_data_lists():
    """
    Read all text files and return their data as separate lists.
    Returns: tuple of (brands, categories, locations)
    """
    brands = read_text_file_to_list('brands.txt')
    categories = read_text_file_to_list('categories.txt')
    locations = read_text_file_to_list('locations.txt')
    
    return brands, categories, locations

def generate_google_maps_queries():
    """
    Generate Google Maps query strings based on the data from all three files.
    Returns a list of query strings.
    """
    brands, categories, locations = get_all_data_lists()
    queries = []
    
    # If categories file is empty or has no data
    if not categories:
        print("Categories file is empty. Generating queries in format: 'brand in location'")
        for brand in brands:
            for location in locations:
                query = f"{brand} in {location}"
                queries.append(query)
    else:
        print("Categories file has data. Generating queries in format: 'brand category in location'")
        for brand in brands:
            for category in categories:
                for location in locations:
                    query = f"{brand} {category} in {location}"
                    queries.append(query)
    
    return queries

def save_queries_to_file(queries, filename="google_maps_queries.txt"):
    """
    Save the generated queries to a text file.
    """
    try:
        with open(filename, 'w', encoding='utf-8') as file:
            for query in queries:
                file.write(f"{query}\n")
        print(f"Queries saved to '{filename}'")
    except Exception as e:
        print(f"Error saving queries to file: {e}")

def main():
    # List of text files to read
    text_files = ['brands.txt', 'categories.txt', 'locations.txt']
    
    # Dictionary to store the data from each file
    data_lists = {}
    
    print("Reading text files and saving data as lists...")
    print("-" * 50)
    
    for filename in text_files:
        if os.path.exists(filename):
            data = read_text_file_to_list(filename)
            data_lists[filename] = data
            print(f"✓ {filename}: {len(data)} items loaded")
            print(f"  Sample data: {data[:3] if data else 'No data'}")
        else:
            print(f"✗ {filename}: File not found")
        print()
    
    # Display summary
    print("Summary of loaded data:")
    print("-" * 30)
    for filename, data in data_lists.items():
        print(f"{filename}: {len(data)} items")
        if data:
            print(f"  First few items: {data[:5]}")
        print()
    
    # You can now use the data_lists dictionary to access the data
    # Example:
    # brands = data_lists['brands.txt']
    # categories = data_lists['categories.txt']
    # locations = data_lists['locations.txt']
    
    return data_lists

if __name__ == "__main__":
    data_lists = main()
    
    # Also demonstrate the simpler function
    print("\n" + "="*50)
    print("Using get_all_data_lists() function:")
    brands, categories, locations = get_all_data_lists()
    print(f"Brands: {brands}")
    print(f"Categories: {categories}")
    print(f"Locations: {locations}")
    
    # Generate Google Maps queries
    print("\n" + "="*50)
    print("Generating Google Maps query strings:")
    queries = generate_google_maps_queries()
    print(f"Total queries generated: {len(queries)}")
    
    # Show first 20 queries
    print("\nFirst 20 queries:")
    for i, query in enumerate(queries[:20], 1):
        print(f"{i:3d}. {query}")
    
    if len(queries) > 20:
        print(f"... and {len(queries) - 20} more queries")
    
    # Save queries to file
    save_queries_to_file(queries)
    
    print(f"\nAll queries saved to 'queries' list variable")
    print(f"Total queries: {len(queries)}")
    
    # Show some statistics
    print(f"\nQuery Statistics:")
    print(f"- Brands: {len(brands)}")
    print(f"- Categories: {len(categories)}")
    print(f"- Locations: {len(locations)}")
    print(f"- Total combinations: {len(brands)} × {len(categories)} × {len(locations)} = {len(queries)}")
    